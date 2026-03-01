//! builder.zig — Z0 offline reverse geocoder binary builder (Zig 0.15).
//!
//! Reads z0_prep.bin and writes z0_geo.bin (RGEO0002 format).
//! Usage: ./builder [z0_prep.bin] [z0_geo.bin]

const std = @import("std");
const Allocator = std.mem.Allocator;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const MAGIC_OUT = "RGEO0002";
const FORMAT_VERSION: u32 = 1;
const GRID_COLS: usize = 1440;
const GRID_ROWS: usize = 720;
const GRID_CELLS: usize = GRID_COLS * GRID_ROWS;
const GRID_CELL_DEG: f64 = 0.25;
const SENTINEL_BOUNDARY: u16 = 0xFFFF;
const BLOCK_RECORDS: usize = 10;
const BLOCK_SIZE: usize = 64;
const BUCKET_DEG: f64 = 2.0;
const BUCKET_COLS: usize = 180;
const BUCKET_ROWS: usize = 90;
const N_BUCKETS: usize = BUCKET_ROWS * BUCKET_COLS;
// Morton quantization: 4096 steps per axis (12-bit), giving ~2.4 km resolution.
// Using 16-bit (65536 steps) produces ~4000 cells per coarse cell → 600M+ records.
// 12-bit produces ~10 cells per coarse cell → ~1.5M records, matching spec size targets.
// Morton codes are still stored as u32; top 8 bits are always zero.
const MORTON_STEPS: u32 = 4096;
const MORTON_STEPS_F: f64 = @floatFromInt(MORTON_STEPS);
const MORTON_MAX: u32 = MORTON_STEPS - 1;

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

const Ring = struct { pts: []const [2]f32 };

const Polygon = struct {
    admin_id: u16,
    rings: []const Ring,
    bbox: [4]f32, // minlon, minlat, maxlon, maxlat
};

const MortonRecord = struct { morton: u32, admin_id: u16 };

// Use a simple growable buffer rather than ArrayList to avoid API churn
const Buf = struct {
    data: []u8,
    len: usize,
    alloc: Allocator,

    fn init(alloc: Allocator) Buf {
        return .{ .data = &.{}, .len = 0, .alloc = alloc };
    }
    fn deinit(self: *Buf) void {
        self.alloc.free(self.data);
    }
    fn ensureCap(self: *Buf, extra: usize) !void {
        const need = self.len + extra;
        if (need <= self.data.len) return;
        const new_cap = @max(need, self.data.len * 2 + 64);
        self.data = try self.alloc.realloc(self.data, new_cap);
    }
    fn append(self: *Buf, bytes: []const u8) !void {
        try self.ensureCap(bytes.len);
        @memcpy(self.data[self.len .. self.len + bytes.len], bytes);
        self.len += bytes.len;
    }
    fn appendByte(self: *Buf, b: u8) !void {
        try self.ensureCap(1);
        self.data[self.len] = b;
        self.len += 1;
    }
    fn wi16(self: *Buf, v: u16) !void {
        var b: [2]u8 = undefined;
        std.mem.writeInt(u16, &b, v, .little);
        try self.append(&b);
    }
    fn wi32(self: *Buf, v: u32) !void {
        var b: [4]u8 = undefined;
        std.mem.writeInt(u32, &b, v, .little);
        try self.append(&b);
    }
    fn wi64(self: *Buf, v: u64) !void {
        var b: [8]u8 = undefined;
        std.mem.writeInt(u64, &b, v, .little);
        try self.append(&b);
    }
    fn slice(self: *const Buf) []const u8 {
        return self.data[0..self.len];
    }
};

// Generic growable typed slice
fn GrowSlice(comptime T: type) type {
    return struct {
        items: []T,
        cap: usize,
        alloc: Allocator,
        const Self = @This();
        fn init(alloc: Allocator) Self {
            return .{ .items = &.{}, .cap = 0, .alloc = alloc };
        }
        fn deinit(self: *Self) void {
            if (self.cap > 0) self.alloc.free(self.items.ptr[0..self.cap]);
        }
        fn push(self: *Self, v: T) !void {
            if (self.items.len == self.cap) {
                const new_cap = @max(self.cap * 2 + 8, 16);
                const new_mem = try self.alloc.realloc(self.items.ptr[0..self.cap], new_cap);
                self.items = new_mem[0..self.items.len];
                self.cap = new_cap;
            }
            self.items.ptr[self.items.len] = v;
            self.items = self.items.ptr[0 .. self.items.len + 1];
        }
        fn toOwnedSlice(self: *Self) []T {
            const s = self.items;
            self.items = &.{};
            self.cap = 0;
            return s;
        }
    };
}

// ---------------------------------------------------------------------------
// Morton code
// ---------------------------------------------------------------------------

fn spreadBits(v: u16) u32 {
    var x: u32 = v;
    x = (x | (x << 8)) & 0x00FF00FF;
    x = (x | (x << 4)) & 0x0F0F0F0F;
    x = (x | (x << 2)) & 0x33333333;
    x = (x | (x << 1)) & 0x55555555;
    return x;
}

fn interleaveBits(lat_q: u16, lon_q: u16) u32 {
    return spreadBits(lat_q) | (spreadBits(lon_q) << 1);
}

// ---------------------------------------------------------------------------
// Binary search helper
// ---------------------------------------------------------------------------

fn containsMorton(slice: []const MortonRecord, morton: u32) bool {
    var lo: usize = 0;
    var hi: usize = slice.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (slice[mid].morton < morton) {
            lo = mid + 1;
        } else if (slice[mid].morton > morton) {
            hi = mid;
        } else {
            return true;
        }
    }
    return false;
}

// ---------------------------------------------------------------------------
// Point-in-polygon — ray casting
// ---------------------------------------------------------------------------

fn pointInRing(lon: f64, lat: f64, pts: []const [2]f32) bool {
    var inside = false;
    var j = pts.len - 1;
    for (pts, 0..) |pi, i| {
        const xi: f64 = @floatCast(pi[0]);
        const yi: f64 = @floatCast(pi[1]);
        const xj: f64 = @floatCast(pts[j][0]);
        const yj: f64 = @floatCast(pts[j][1]);
        if ((yi > lat) != (yj > lat) and lon < (xj - xi) * (lat - yi) / (yj - yi) + xi)
            inside = !inside;
        j = i;
    }
    return inside;
}

fn pointInPolygon(lon: f64, lat: f64, poly: *const Polygon) bool {
    if (!pointInRing(lon, lat, poly.rings[0].pts)) return false;
    for (poly.rings[1..]) |ring| if (pointInRing(lon, lat, ring.pts)) return false;
    return true;
}

// ---------------------------------------------------------------------------
// Spatial index — 2° bucket grid
// ---------------------------------------------------------------------------

const SpatialIndex = struct {
    polys: []const Polygon,
    // flat bucket array: index = row * BUCKET_COLS + col
    // each bucket is a slice into `pool`
    bucket_starts: []u32, // start index in pool for each bucket
    bucket_lens: []u32,
    pool: []u32, // all poly indices packed together
    alloc: Allocator,

    fn init(alloc: Allocator, polys: []const Polygon) !SpatialIndex {
        // First pass: count entries per bucket
        const bucket_counts = try alloc.alloc(u32, N_BUCKETS);
        defer alloc.free(bucket_counts);
        @memset(bucket_counts, 0);

        for (polys) |*p| {
            const c0: usize = @intFromFloat(@max(0.0, (p.bbox[0] + 180.0) / BUCKET_DEG));
            const c1: usize = @min(BUCKET_COLS - 1, @as(usize, @intFromFloat((p.bbox[2] + 180.0) / BUCKET_DEG)));
            const r0: usize = @intFromFloat(@max(0.0, (p.bbox[1] + 90.0) / BUCKET_DEG));
            const r1: usize = @min(BUCKET_ROWS - 1, @as(usize, @intFromFloat((p.bbox[3] + 90.0) / BUCKET_DEG)));
            var r = r0;
            while (r <= r1) : (r += 1) {
                var c = c0;
                while (c <= c1) : (c += 1) bucket_counts[r * BUCKET_COLS + c] += 1;
            }
        }

        // Compute starts (prefix sum)
        const bucket_starts = try alloc.alloc(u32, N_BUCKETS);
        const bucket_lens = try alloc.alloc(u32, N_BUCKETS);
        var total: u32 = 0;
        for (0..N_BUCKETS) |i| {
            bucket_starts[i] = total;
            bucket_lens[i] = 0;
            total += bucket_counts[i];
        }

        // Allocate pool and fill
        const pool = try alloc.alloc(u32, total);
        for (polys, 0..) |*p, pi| {
            const c0: usize = @intFromFloat(@max(0.0, (p.bbox[0] + 180.0) / BUCKET_DEG));
            const c1: usize = @min(BUCKET_COLS - 1, @as(usize, @intFromFloat((p.bbox[2] + 180.0) / BUCKET_DEG)));
            const r0: usize = @intFromFloat(@max(0.0, (p.bbox[1] + 90.0) / BUCKET_DEG));
            const r1: usize = @min(BUCKET_ROWS - 1, @as(usize, @intFromFloat((p.bbox[3] + 90.0) / BUCKET_DEG)));
            var r = r0;
            while (r <= r1) : (r += 1) {
                var c = c0;
                while (c <= c1) : (c += 1) {
                    const bi = r * BUCKET_COLS + c;
                    pool[bucket_starts[bi] + bucket_lens[bi]] = @intCast(pi);
                    bucket_lens[bi] += 1;
                }
            }
        }

        return .{
            .polys = polys,
            .bucket_starts = bucket_starts,
            .bucket_lens = bucket_lens,
            .pool = pool,
            .alloc = alloc,
        };
    }

    fn deinit(self: *SpatialIndex) void {
        self.alloc.free(self.bucket_starts);
        self.alloc.free(self.bucket_lens);
        self.alloc.free(self.pool);
    }

    fn query(self: *const SpatialIndex, lon: f64, lat: f64) ?u16 {
        const r: usize = @min(BUCKET_ROWS - 1, @as(usize, @intFromFloat(@max(0.0, (lat + 90.0) / BUCKET_DEG))));
        const c: usize = @min(BUCKET_COLS - 1, @as(usize, @intFromFloat(@max(0.0, (lon + 180.0) / BUCKET_DEG))));
        const bi = r * BUCKET_COLS + c;
        const start = self.bucket_starts[bi];
        const end = start + self.bucket_lens[bi];
        for (self.pool[start..end]) |pi| {
            if (pointInPolygon(lon, lat, &self.polys[pi])) return self.polys[pi].admin_id;
        }
        return null;
    }
};

// ---------------------------------------------------------------------------
// Prep file reader
// ---------------------------------------------------------------------------

const PrepData = struct {
    num_admins: u32,
    name_zstd: []u8,
    admin_bytes: []u8,
    polys: []Polygon,
    _raw: []u8,
    _coord_arena: std.heap.ArenaAllocator,

    fn deinit(self: *PrepData, alloc: Allocator) void {
        self._coord_arena.deinit();
        alloc.free(self.name_zstd);
        alloc.free(self.admin_bytes);
        alloc.free(self.polys);
        alloc.free(self._raw);
    }
};

fn ri32(data: []const u8, pos: *usize) u32 {
    const v = std.mem.readInt(u32, data[pos.*..][0..4], .little);
    pos.* += 4;
    return v;
}
fn ri16(data: []const u8, pos: *usize) u16 {
    const v = std.mem.readInt(u16, data[pos.*..][0..2], .little);
    pos.* += 2;
    return v;
}
fn rf32(data: []const u8, pos: *usize) f32 {
    const v: f32 = @bitCast(std.mem.readInt(u32, data[pos.*..][0..4], .little));
    pos.* += 4;
    return v;
}

fn readPrepFile(alloc: Allocator, path: []const u8) !PrepData {
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();
    const raw = try file.readToEndAlloc(alloc, 4 * 1024 * 1024 * 1024);
    errdefer alloc.free(raw);

    var pos: usize = 0;
    if (!std.mem.eql(u8, raw[pos .. pos + 8], "Z0PREP01")) return error.BadMagic;
    pos += 8;

    const num_admins = ri32(raw, &pos);
    const num_polys = ri32(raw, &pos);
    const name_zstd_len = ri32(raw, &pos);
    const admin_table_len = ri32(raw, &pos);

    const name_zstd = try alloc.dupe(u8, raw[pos .. pos + name_zstd_len]);
    pos += name_zstd_len;
    const admin_bytes = try alloc.dupe(u8, raw[pos .. pos + admin_table_len]);
    pos += admin_table_len;

    var coord_arena = std.heap.ArenaAllocator.init(alloc);
    const ca = coord_arena.allocator();
    const polys = try alloc.alloc(Polygon, num_polys);

    std.debug.print("  Reading {d} polygon parts …\n", .{num_polys});
    var last_pct: usize = 0;
    for (polys, 0..) |*poly, pi| {
        poly.admin_id = ri16(raw, &pos);
        const num_rings = ri32(raw, &pos);
        const rings = try ca.alloc(Ring, num_rings);
        var bbox = [4]f32{ 1e9, 1e9, -1e9, -1e9 };
        for (rings, 0..) |*ring, ri| {
            const np = ri32(raw, &pos);
            const pts = try ca.alloc([2]f32, np);
            for (pts) |*pt| {
                pt[0] = rf32(raw, &pos);
                pt[1] = rf32(raw, &pos);
            }
            ring.pts = pts;
            if (ri == 0) for (pts) |pt| {
                if (pt[0] < bbox[0]) bbox[0] = pt[0];
                if (pt[1] < bbox[1]) bbox[1] = pt[1];
                if (pt[0] > bbox[2]) bbox[2] = pt[0];
                if (pt[1] > bbox[3]) bbox[3] = pt[1];
            };
        }
        poly.rings = rings;
        poly.bbox = bbox;
        const pct = (pi + 1) * 100 / num_polys;
        if (pct >= last_pct + 10) {
            std.debug.print("    {d}%\r", .{pct});
            last_pct = pct;
        }
    }
    std.debug.print("    done ({d})\n", .{num_polys});
    return .{ .num_admins = num_admins, .name_zstd = name_zstd, .admin_bytes = admin_bytes, .polys = polys, ._raw = raw, ._coord_arena = coord_arena };
}

// ---------------------------------------------------------------------------
// Layer 0: coarse grid
// ---------------------------------------------------------------------------

const CoarseGrid = struct {
    bitmap: []u8,
    rank_table: []u32,
    values: []u16,
    boundary_cells: [][2]u16,
};

fn buildCoarseGrid(alloc: Allocator, index: *const SpatialIndex) !CoarseGrid {
    std.debug.print("  Classifying {d} coarse cells …\n", .{GRID_CELLS});
    const OCEAN_SENTINEL: u16 = 0xFFFF;
    const cell_admin = try alloc.alloc(u16, GRID_CELLS);
    defer alloc.free(cell_admin);
    @memset(cell_admin, OCEAN_SENTINEL);

    // Quarter-cell offset for sub-sampling (0.0625° ≈ 7 km).
    const Q: f64 = GRID_CELL_DEG * 0.25;

    var last_pct: usize = 0;
    for (0..GRID_ROWS) |row| {
        const lat_c: f64 = 90.0 - (@as(f64, @floatFromInt(row)) + 0.5) * GRID_CELL_DEG;
        for (0..GRID_COLS) |col| {
            const lon_c: f64 = -180.0 + (@as(f64, @floatFromInt(col)) + 0.5) * GRID_CELL_DEG;
            // Sample center first; fall back to 4 quadrant sub-centers so that
            // coastal / island cells whose centroid falls in the ocean are still
            // classified as land.  First hit wins (center takes priority).
            const samples = [5][2]f64{
                .{ lon_c,     lat_c     },
                .{ lon_c - Q, lat_c - Q },
                .{ lon_c + Q, lat_c - Q },
                .{ lon_c - Q, lat_c + Q },
                .{ lon_c + Q, lat_c + Q },
            };
            for (samples) |pt| {
                if (index.query(pt[0], pt[1])) |aid| {
                    cell_admin[row * GRID_COLS + col] = aid;
                    break;
                }
            }
        }
        const pct = (row + 1) * 100 / GRID_ROWS;
        if (pct >= last_pct + 5) {
            std.debug.print("    {d}%\r", .{pct});
            last_pct = pct;
        }
    }
    std.debug.print("    done.\n", .{});

    const bitmap_bytes_raw = (GRID_CELLS + 7) / 8;
    const bitmap_bytes = ((bitmap_bytes_raw + 63) / 64) * 64;
    const bitmap = try alloc.alloc(u8, bitmap_bytes);
    @memset(bitmap, 0);

    var values = GrowSlice(u16).init(alloc);
    var boundary = GrowSlice([2]u16).init(alloc);

    for (0..GRID_ROWS) |row| {
        for (0..GRID_COLS) |col| {
            const idx = row * GRID_COLS + col;
            const aid = cell_admin[idx];
            if (aid == OCEAN_SENTINEL) continue;

            bitmap[idx / 8] |= @as(u8, 1) << @intCast(idx % 8);

            var all_same = true;
            outer: for ([_]i32{ -1, 0, 1 }) |dr| {
                for ([_]i32{ -1, 0, 1 }) |dc| {
                    if (dr == 0 and dc == 0) continue;
                    const nr: i32 = @as(i32, @intCast(row)) + dr;
                    if (nr < 0 or nr >= @as(i32, GRID_ROWS)) continue;
                    const nc: i32 = @mod(@as(i32, @intCast(col)) + dc, @as(i32, GRID_COLS));
                    const nidx: usize = @as(usize, @intCast(nr)) * GRID_COLS + @as(usize, @intCast(nc));
                    if (cell_admin[nidx] != aid) { all_same = false; break :outer; }
                }
            }

            if (all_same) {
                try values.push(aid);
            } else {
                try values.push(SENTINEL_BOUNDARY);
                try boundary.push(.{ @intCast(row), @intCast(col) });
            }
        }
    }

    const num_rank_blocks = (GRID_CELLS + 511) / 512;
    const rank_table = try alloc.alloc(u32, num_rank_blocks);
    var cumulative: u32 = 0;
    for (0..num_rank_blocks) |bi| {
        rank_table[bi] = cumulative;
        const sb = bi * 64;
        const eb = @min(sb + 64, bitmap_bytes);
        for (bitmap[sb..eb]) |b| cumulative += @popCount(b);
    }

    const land = values.items.len;
    const bc = boundary.items.len;
    std.debug.print("  Land: {d}, Interior: {d}, Boundary: {d}\n", .{ land, land - bc, bc });

    return .{
        .bitmap = bitmap,
        .rank_table = rank_table,
        .values = values.toOwnedSlice(),
        .boundary_cells = boundary.toOwnedSlice(),
    };
}

// ---------------------------------------------------------------------------
// Layer 1: Morton table — threaded
// ---------------------------------------------------------------------------

const MortonWorkCtx = struct {
    cells: [][2]u16,
    index: *const SpatialIndex,
    records: GrowSlice(MortonRecord),
    progress: *std.atomic.Value(usize),
};

fn mortonWorker(ctx: *MortonWorkCtx) void {
    for (ctx.cells) |cell| {
        const row: usize = cell[0];
        const col: usize = cell[1];

        const lat_hi: f64 = 90.0 - @as(f64, @floatFromInt(row)) * GRID_CELL_DEG;
        const lat_lo: f64 = 90.0 - @as(f64, @floatFromInt(row + 1)) * GRID_CELL_DEG;
        const lon_lo: f64 = -180.0 + @as(f64, @floatFromInt(col)) * GRID_CELL_DEG;
        const lon_hi: f64 = -180.0 + @as(f64, @floatFromInt(col + 1)) * GRID_CELL_DEG;

        const lq_lo: u32 = @intFromFloat(@max(0.0, (lat_lo + 90.0) / 180.0 * MORTON_STEPS_F));
        const lq_hi: u32 = @min(MORTON_MAX, @as(u32, @intFromFloat((lat_hi + 90.0) / 180.0 * MORTON_STEPS_F)));
        const aq_lo: u32 = @intFromFloat(@max(0.0, (lon_lo + 180.0) / 360.0 * MORTON_STEPS_F));
        const aq_hi: u32 = @min(MORTON_MAX, @as(u32, @intFromFloat((lon_hi + 180.0) / 360.0 * MORTON_STEPS_F)));

        var lq = lq_lo;
        while (lq <= lq_hi) : (lq += 1) {
            const lat_c: f64 = (@as(f64, @floatFromInt(lq)) + 0.5) / MORTON_STEPS_F * 180.0 - 90.0;
            var aq = aq_lo;
            while (aq <= aq_hi) : (aq += 1) {
                const lon_c: f64 = (@as(f64, @floatFromInt(aq)) + 0.5) / MORTON_STEPS_F * 360.0 - 180.0;
                if (ctx.index.query(lon_c, lat_c)) |aid| {
                    ctx.records.push(.{
                        .morton = interleaveBits(@truncate(lq), @truncate(aq)),
                        .admin_id = aid,
                    }) catch {};
                }
            }
        }
        _ = ctx.progress.fetchAdd(1, .monotonic);
    }
}

fn buildMortonTable(alloc: Allocator, boundary_cells: [][2]u16, index: *const SpatialIndex) ![]MortonRecord {
    const n_cells = boundary_cells.len;
    const n_threads = std.Thread.getCpuCount() catch 4;
    std.debug.print("  Morton table: {d} boundary cells, {d} threads\n", .{ n_cells, n_threads });
    const chunk = (n_cells + n_threads - 1) / n_threads;

    var progress = std.atomic.Value(usize).init(0);
    const ctxs = try alloc.alloc(MortonWorkCtx, n_threads);
    defer alloc.free(ctxs);
    const threads = try alloc.alloc(std.Thread, n_threads);
    defer alloc.free(threads);

    for (0..n_threads) |ti| {
        const start = ti * chunk;
        const end = @min(start + chunk, n_cells);
        ctxs[ti] = .{
            .cells = if (start < end) boundary_cells[start..end] else &.{},
            .index = index,
            .records = GrowSlice(MortonRecord).init(alloc),
            .progress = &progress,
        };
        threads[ti] = try std.Thread.spawn(.{}, mortonWorker, .{&ctxs[ti]});
    }

    var last_pct: usize = 0;
    while (true) {
        const done = progress.load(.monotonic);
        const pct = if (n_cells > 0) done * 100 / n_cells else 100;
        if (pct >= last_pct + 5) {
            std.debug.print("    {d}%  ({d}/{d})\r", .{ pct, done, n_cells });
            last_pct = pct;
        }
        if (done >= n_cells) break;
        std.Thread.sleep(100 * std.time.ns_per_ms);
    }
    for (threads) |t| t.join();
    std.debug.print("    100%\n", .{});

    // Merge all thread results
    var total_count: usize = 0;
    for (ctxs) |*ctx| total_count += ctx.records.items.len;
    const all = try alloc.alloc(MortonRecord, total_count);
    var off: usize = 0;
    for (ctxs) |*ctx| {
        @memcpy(all[off .. off + ctx.records.items.len], ctx.records.items);
        off += ctx.records.items.len;
        ctx.records.deinit();
    }

    std.debug.print("  Sorting {d} Morton records …\n", .{all.len});
    std.mem.sort(MortonRecord, all, {}, struct {
        pub fn lessThan(_: void, a: MortonRecord, b: MortonRecord) bool {
            return a.morton < b.morton;
        }
    }.lessThan);

    // Deduplicate (same morton → keep first)
    var deduped = GrowSlice(MortonRecord).init(alloc);
    var prev: u32 = std.math.maxInt(u32);
    for (all) |rec| {
        if (rec.morton != prev) {
            try deduped.push(rec);
            prev = rec.morton;
        }
    }
    alloc.free(all);
    std.debug.print("  After dedup: {d} Morton records\n", .{deduped.items.len});

    // Quad-tree compression: group records by parent (morton >> 2).
    // If all 4 children of a parent are present with the same admin_id,
    // replace them with a single parent record.
    // The query uses 1 level of parent fallback (morton >> 2), so this is
    // exactly the right compression: parent records are found on the second lookup.
    const pre_prune = deduped.items;
    var pruned = GrowSlice(MortonRecord).init(alloc);
    var i: usize = 0;
    while (i < pre_prune.len) {
        const parent = pre_prune[i].morton >> 2;
        // Find all consecutive records sharing this parent
        var j = i;
        while (j < pre_prune.len and pre_prune[j].morton >> 2 == parent) j += 1;
        const group = pre_prune[i..j];

        // Compress if group is exactly the 4 children (morton & 3 == 0,1,2,3)
        // in order, all with the same admin_id.
        const can_compress = (group.len == 4) and
            ((group[0].morton & 3) == 0) and
            ((group[1].morton & 3) == 1) and
            ((group[2].morton & 3) == 2) and
            ((group[3].morton & 3) == 3) and
            (group[0].admin_id == group[1].admin_id) and
            (group[1].admin_id == group[2].admin_id) and
            (group[2].admin_id == group[3].admin_id);

        // Only compress if parent Morton code doesn't already exist as a direct
        // PIP result.  The parent code (child >> 2) can collide with a cell from
        // a geographically unrelated region (e.g. Central-Asia cells at double
        // resolution share the same parent Morton code as South-American cells at
        // normal resolution).  If parent is already in pre_prune we keep the
        // children unchanged so neither record is overwritten by the wrong region.
        if (can_compress and !containsMorton(pre_prune[0..i], parent)) {
            try pruned.push(.{ .morton = parent, .admin_id = group[0].admin_id });
        } else {
            for (group) |r| try pruned.push(r);
        }
        i = j;
    }
    { var tmp = deduped; tmp.deinit(); }
    std.debug.print("  After quad-tree compression: {d} Morton records\n", .{pruned.items.len});

    // Re-sort after compression: parent records (smaller codes) may have been
    // emitted after child records, breaking the sorted order.
    const result = pruned.toOwnedSlice();
    std.mem.sort(MortonRecord, result, {}, struct {
        pub fn lessThan(_: void, a: MortonRecord, b: MortonRecord) bool {
            return a.morton < b.morton;
        }
    }.lessThan);
    return result;
}

// ---------------------------------------------------------------------------
// Pack Morton records into 64-byte blocks
// ---------------------------------------------------------------------------

const PackedMorton = struct { block_data: []u8, directory: []u32 };

fn packMortonBlocks(alloc: Allocator, records: []const MortonRecord) !PackedMorton {
    const n_blocks = (records.len + BLOCK_RECORDS - 1) / BLOCK_RECORDS;
    const block_data = try alloc.alloc(u8, n_blocks * BLOCK_SIZE);
    @memset(block_data, 0);
    const directory = try alloc.alloc(u32, n_blocks);

    for (0..n_blocks) |bi| {
        directory[bi] = records[bi * BLOCK_RECORDS].morton;
        const s = bi * BLOCK_RECORDS;
        const e = @min(s + BLOCK_RECORDS, records.len);
        for (records[s..e], 0..) |rec, ri| {
            const off = bi * BLOCK_SIZE + ri * 6;
            std.mem.writeInt(u32, block_data[off..][0..4], rec.morton, .little);
            std.mem.writeInt(u16, block_data[off + 4 ..][0..2], rec.admin_id, .little);
        }
    }
    return .{ .block_data = block_data, .directory = directory };
}

// ---------------------------------------------------------------------------
// Write RGEO0002 binary
// ---------------------------------------------------------------------------

fn writeBinaryFile(
    alloc: Allocator,
    path: []const u8,
    grid: *const CoarseGrid,
    pm: *const PackedMorton,
    morton_record_count: u32,
    admin_bytes: []const u8,
    name_zstd: []const u8,
) !void {
    var out = Buf.init(alloc);
    defer out.deinit();

    const bitmap_bytes: u64 = grid.bitmap.len;
    const rank_bytes: u64 = grid.rank_table.len * 4;
    const values_bytes: u64 = grid.values.len * 2;
    const block_bytes: u64 = pm.block_data.len;
    const dir_bytes: u64 = pm.directory.len * 4;
    const admin_len: u64 = admin_bytes.len;
    const name_len: u64 = name_zstd.len;

    const HEADER_SIZE: u64 = 64;
    const bm_off: u64 = HEADER_SIZE;
    const rk_off: u64 = bm_off + bitmap_bytes;
    const vl_off: u64 = rk_off + rank_bytes;
    const mb_off: u64 = vl_off + values_bytes;
    const md_off: u64 = mb_off + block_bytes;
    const ad_off: u64 = md_off + dir_bytes;
    const nm_off: u64 = ad_off + admin_len;
    const total: u64 = nm_off + name_len;

    std.debug.print("  Bitmap:{d} Rank:{d} Values:{d} Morton:{d}+{d} Admin:{d} Names:{d}\n", .{ bitmap_bytes, rank_bytes, values_bytes, block_bytes, dir_bytes, admin_len, name_len });
    std.debug.print("  TOTAL: {d} bytes ({d:.2} MB)\n", .{ total, @as(f64, @floatFromInt(total)) / 1_048_576.0 });

    // Header (64 bytes)
    try out.append(MAGIC_OUT);
    try out.wi32(FORMAT_VERSION);
    try out.wi32(@truncate(@as(u64, @bitCast(std.time.timestamp()))));
    try out.wi32(@truncate(bm_off));
    try out.wi32(@truncate(rk_off));
    try out.wi32(@truncate(vl_off));
    try out.wi32(@intCast(grid.values.len));
    try out.wi32(@truncate(mb_off));
    try out.wi32(@truncate(md_off));
    try out.wi32(morton_record_count);
    try out.wi32(@intCast(pm.directory.len));
    try out.wi32(@truncate(ad_off));
    try out.wi32(@truncate(nm_off));
    try out.wi64(0);
    std.debug.assert(out.len == HEADER_SIZE);

    // Sections
    try out.append(grid.bitmap);
    for (grid.rank_table) |v| try out.wi32(v);
    for (grid.values) |v| try out.wi16(v);
    try out.append(pm.block_data);
    for (pm.directory) |v| try out.wi32(v);
    try out.append(admin_bytes);
    try out.append(name_zstd);

    const fh = try std.fs.cwd().createFile(path, .{});
    defer fh.close();
    try fh.writeAll(out.slice());
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const args = try std.process.argsAlloc(alloc);
    defer std.process.argsFree(alloc, args);

    const prep_path = if (args.len > 1) args[1] else "z0_prep.bin";
    const out_path = if (args.len > 2) args[2] else "z0_geo.bin";
    const t0 = std.time.milliTimestamp();

    std.debug.print("\nStep 1: Reading {s} …\n", .{prep_path});
    var prep = try readPrepFile(alloc, prep_path);
    defer prep.deinit(alloc);
    std.debug.print("  {d} admins, {d} polygons\n", .{ prep.num_admins, prep.polys.len });

    std.debug.print("\nStep 2: Spatial index …\n", .{});
    var index = try SpatialIndex.init(alloc, prep.polys);
    defer index.deinit();

    std.debug.print("\nStep 3: Coarse grid …\n", .{});
    const grid = try buildCoarseGrid(alloc, &index);

    std.debug.print("\nStep 4: Morton table …\n", .{});
    const morton_records = try buildMortonTable(alloc, grid.boundary_cells, &index);
    const pm = try packMortonBlocks(alloc, morton_records);

    std.debug.print("\nStep 5: Writing {s} …\n", .{out_path});
    try writeBinaryFile(alloc, out_path, &grid, &pm, @intCast(morton_records.len), prep.admin_bytes, prep.name_zstd);

    const elapsed = std.time.milliTimestamp() - t0;
    std.debug.print("\nDone in {d:.1}s\n", .{@as(f64, @floatFromInt(elapsed)) / 1000.0});
}
