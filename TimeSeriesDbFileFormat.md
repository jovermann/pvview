TimeSeriesDB file format
===================

TimeSeriesDB is a simple filesystem based database for time series data
like sensor data or PV inverter data.

Files
-----
One file per UTC day. New files start at 00:00:00 UTC. Each file contains multiple time series.
Original data and downsampled data in various granularity are stored in separate files, to allow manual deletion of old high resolution data.

data_YYYY-MM-DD.tsdb: Original data. Not downsampled.
dsda_YYYY-MM-DD.1s.tsdb: Downsampled to 1s buckets. No min/avg/max, just plain values.
dsda_YYYY-MM-DD.5s.tsdb: Downsampled to 5s buckets. Min/avg/max.
dsda_YYYY-MM-DD.15s.tsdb: Downsampled to 15s buckets. Min/avg/max.
dsda_YYYY-MM-DD.1m.tsdb: Downsampled to 1m buckets. Min/avg/max.
dsda_YYYY-MM-DD.5m.tsdb: Downsampled to 5m buckets. Min/avg/max.
dsda_YYYY-MM-DD.15m.tsdb: Downsampled to 15m buckets. Min/avg/max.
dsda_YYYY-MM-DD.1h.tsdb: Downsampled to 1h buckets. Min/avg/max.

Binary format
-------------
All data is stored in little-endian format. int8_t to int64_t and
uint8_t to uint64_t specify signed and unsigned integers of the respective size.
For clarity in this spec: 8-bit = uint8_t/int8_t, 16-bit = uint16_t/int16_t, 24-bit = uint24_t/int24_t.
int24_t and uint24_t denote 3-byte integer values.
int24_t should get sign extended when stored in a 32 or 64 bit variable.

The file consists of a header and then a sequence of entries. Each entry either sets or advances the current timestamp, defines a new channel and its ID, or adds a new data value.

uint64_t tag; // Must be bytes 54 53 44 42 00 00 00 00 (ASCII "TSDB\0\0\0\0").
uint32_t version; // 1
Entry entries[]; // Entries extend to the end of file. Entries are of different size. The size depends on the entry type.

Entry
-----
uint8_t type; // Type determines the layout of entry. ValueEntry, TimeEntry, ChannelDefinitionEntry or SpecialEntry.
uint8_t data[]; // The semantics and layout of data depends on the type and is described below.

ValueEntries
------------
uint8_t type = 0..0xef; // channelId: The channel id must be defined by a type=0xf5 channel definition beforehand. Also the timestamp must be set beforehand.
uint8_t data[]; // Data in the format defined for this channel id (see type=0xf5). For downsampled data (dsBucketMs present) this is min/avg/max (three items of the defined format). For non-downsampled data (dsBucketMs not present) this is the value.

uint8_t type = 0xff; // Escape for 16-bit (uint16_t) channel id. The 16 bit channel id must be defined by a type=0xf6 channel definition beforehand. Also the timestamp must be set beforehand.
uint16_t channelId16; // 16-bit channel id 0xf0..0xffff
uint8_t data[]; // Data in the format defined for this channel id (see type=0xf6). For downsampled data (dsBucketMs present) this is min/avg/max (three items of the defined format). For non-downsampled data (dsBucketMs not present) this is the value.

TimeEntries
-----------
uint8_t type = 0xf0;
uint64_t timeAbsolute; // Set timestamp to absolute UNIX time in milliseconds UTC.
uint8_t type = 0xf1;
uint8_t timeRelative8; // Advance timestamp forward (uint8_t).
uint8_t type = 0xf2;
uint16_t timeRelative16; // Advance timestamp forward (uint16_t).
uint8_t type = 0xf3;
uint24_t timeRelative24; // Advance timestamp forward (uint24_t).
uint8_t type = 0xf4;
uint32_t timeRelative32; // Advance timestamp forward (uint32_t).

ChannelDefinitionEntries
------------------------
uint8_t type = 0xf5; // Channel definition (8-bit channel id).
uint8_t channelId; // Channel id (0..0xef).
uint8_t formatId; // Format of the value of ValueEntries for this channel (see table below).
uint8_t nameLen; // Length of the name in bytes.
uint8_t name[nameLen]; // Name of this channel in UTF-8.
uint8_t type = 0xf6; // Channel definition (16-bit channel id).
uint16_t channelId16; // Channel id (0xf0..0xffff).
uint8_t formatId; // Format of the value of ValueEntries for this channel (see table below).
uint8_t nameLen; // Length of the name in bytes.
uint8_t name[nameLen]; // Name of this channel in UTF-8.

MetaInfoEntries
---------------
Meta-info entries (key/value pairs) may occur anywhere in a TSDB file, but global meta-info entries always occur before the
first ChannelDefinitionEntry and before the first TimeEntry. If a certain meta-info entry did not occur before the first
ChannelDefinitionEntry and before the first TimeEntry it can be safely assumed that this meta-info entry is not present
and should thus have its default value/semantics.

uint8_t type = 0xf7; // Meta-info entry, a key/value pair where key is a UTF-8 string and value is a signed/unsigned integer, a float/double or a string.
uint8_t keyLen; // Length of the key name in bytes. 0..255 bytes.
uint8_t key[keyLen]; // Key of this key/value pair in UTF-8.
uint8_t formatId; // Format of the value. See format ids.
uint8_t value[]; // Value in the format described by formatId.

Defined MetaInfoEntry keys
--------------------------
key dsBucketMs, value N (any integer format): Global meta-info: If this is present, then this file contains downsampled time series, downsampled to a bucket size of N ms (N > 0).
    If this is not present then this file does not contain downsampled data.
    The bucket size is always a whole fraction of 1d. Typical bucket sizes used are 5s, 15s, 1m, 5m, 15m, 1h.
    When this key is present the layout for numeric values changes from a simple value:
    The layout of data[] for numeric ValueEntries is min/avg/max (minimum, average and maximum in the current bucket). The format id is always the same as the original data and this format is used for min/avg/max.
    Avg is the arithmetic average and is accumulated as double and is quantized to the nearest value upon serialization, potentially losing fractional information. It is rounded towards 0 when tied.
    String values are assigned to the bucket covering their timestamp and thus their timestamp gets changed to the center timestamp of the bucket. The string itself is unchanged. If multiple strings for the same channel occur in the same bucket, only the last string put into the bucket (last write wins semantics). For Series Array Entries, all strings are discarded.
    The timestamp of each bucket is the center timestamp, rounded down to the nearest ms.
    The first bucket of a day starts at 00:00:00 UTC which is also the starting time of each day file.
    Bucket N on the UTC day of the file is associated with all timestamps [N*dsBucketMs, (N+1)*dsBucketMs) of that UTC day.
    Empty buckets are omitted. Their center timestamp is not present in the file.

Series Array Entry
------------------
Series Array Entry can only represent numeric values. Strings are discarded upon conversion. Series which only contain strings are discarded.

A Series Array Entry contains all information for a series for a whole day. It is used to achieve compact completed day files. It is not suitable for incremental writing.
A day is split into numTimeSlots time slots. Each time slot is 24h / numTimeSlots long. The time stamp of each time slot is its center time stamp, rounded down to the nearest ms.
Each time slot either contains a single value or a min/avg/max triplet. In addition to values or triplets, a time slot may be void, indicated by either a single void value or a void/void/void triplet.

All values in each series array are delta encoded to make the numbers small. The starting "last" value per series is 0 and all values of all chunks are iteratively added to the last value to get the actual value and then this becomes the last value.
This delta encoding happens across chunk boundaries (no reset to 0 at the start of chunks) and across min/avg/max (as if these were 3 sequential values). The last value stays unmodified across void values.

Void values are always encoded as -64 to make the encoding of void values one byte long.
Whenever a real delta value of -64 needs to be encoded, this is encoded by minus64 instead, where minus64 is any ZigZag encoded number that does otherwise not occur in this series.

Sequences of the same value (void or any delta value) are run length encoded to compress the data. The data consists of a sequence of chunks where each chunk represents a sequence of void or non-void delta values.

uint8_t type = 0xf8;    // Series Array: All values of a series, covering the whole day.
LEB128 entrySize;       // Size of this entry in bytes including the type byte. This allows to skip the section.
LEB128 nameLen;         // Length of the series name.
uint8_t name[nameLen];  // Series name in UTF-8. Each series name must occur only once in a file.
LEB128 numTimeSlots;    // Number of time slots.
LEB128 nDecimals;      // Number of decimals of each value. 0 means real_value = value, 1 means real_value = value / 10, 2 means real_value = value / 100 and so on. Valid values are 0..3. Other values are reserved.
LEB128 valuesPerTimeSlot; // Number of values per time slot: 1 means plain value, 3 means min/avg/max triplet (downsampled data). Valid values are 1 and 3. Other values are reserved.
ZigZag minus64;         // This value is used to indicate a non-void delta value of -64. Upon encoding, this must be set to any unused delta value (except -64), regardless whether the delta -64 occurs in the series or not.
SeriesArrayChunk chunks[]; // Array of chunks that represent numTimeSlots*valuesPerTimeSlot scalar values.

SeriesArrayChunk: Each SeriesArrayChunk is either a DeltaChunk, RepDeltaChunk or VoidChunk. These define the values for a sequence of time slots. Each SeriesArrayChunk starts with the typeAndLen, which defines the type and the length of a SeriesArrayChunk:
ZigZag typeAndLen; // Defines type and length of the chunk:

If typeAndLen > 0: DeltaChunk: N=typeAndLen. This chunk represents N time slots.
    ZigZag delta[typeAndLen * valuesPerTimeSlot]; // Array of delta values and/or void values.
If typeAndLen < 0 and (typeAndLen & 1) == 0: RepDeltaChunk: N = (-typeAndLen) >> 1. This chunk represents N non-void time slots with a constant delta value.
    ZigZag delta; // Delta value.
        If valuesPerTimeSlot == 1: This is the delta value for the next N time slots.
        If valuesPerTimeSlot == 3: This is the (delta, delta, delta) triplet for the next N time slots.
If typeAndLen < -1 && typeAndLen & 1 == 1: RepVoid: N = (-typeAndLen) >> 1. This chunk represents N void time slots.
If typeAndLen == -1 or typeAndLen == 0: Reserved encoding. Not used by encoding. Invalid upon decoding.

N is always counted in time slots.

The encoding of "chunks" happens in these sequential steps:
- Concatenation: Input: Array of values and void values. For valuesPerTimeSlot == 3: Concatenate all triplets.
- Delta calculation: Calculate the delta of all values, ignoring void values. The initial value for the delta encoding is 0, so the first delta value is always the value itself.
- Find the unused delta value with the shortest ZigZag+LEB128 encoding length and use this as minus64. If there are multiple encodings with the same shortest length, choose any.
- Replace all delta values of -64 with minus64.
- Replace all void values with -64.
- RLE encoding: Encode sequences of void or non-void values into DeltaChunks, except:
  - Encode sequences of the same non-void delta value into RepDeltaChunk for 3 or more identical non-void delta values.
  - Encode sequences of void values into VoidChunk for 3 or more void delta values.

The number of decimals of the input data is preserved, except that values with > 3 decimals get rounded to 3 decimals.

Files containing Series Array Entries (after the header) only contain Series Array Entries and no MetaInfoEntries, no TimeEntries, no ValueEntries and no ChannelDefinitionEntries.

Downsampling
------------

When downsampling data into bucket or Series Arrays, the following applies:
When downsampling into a time slot with just one value, this should be the arithmetic average, rounded to the same precision (decimals) as the input values.
When downsampling into min/avg/max, min and max should be the minimum and maximum values of the input values, and avg should be the arithmetic average, rounded to the same precision (decimals) as the input values.

Format ids
----------
0x00: float value;
0x01: double value; With the display hint to display 0 decimals.
0x02: double value; With the display hint to display 1 decimal.
0x03: double value; With the display hint to display 2 decimals.
0x04: double value; With the display hint to display 3 decimals.
0x05: double value; With the display hint to display 4 decimals.
0x06: double value; With the display hint to display 5 decimals.
0x07: double value; With the display hint to display 6 or more decimals.
0x08: String (UTF-8, 8-bit length): uint8_t len; uint8_t str[len];
0x09: String (UTF-8, 16-bit length): uint16_t len; uint8_t str[len];
0x0a: String (UTF-8, 32-bit length): uint32_t len; uint8_t str[len];
0x0b: String (UTF-8, 64-bit length): uint64_t len; uint8_t str[len];
0x10: int8_t value;
0x11: int8_t x; double value = x / 10.0;
0x12: int8_t x; double value = x / 100.0;
0x13: int8_t x; double value = x / 1000.0;
0x20: int16_t value;
0x21: int16_t x; double value = x / 10.0;
0x22: int16_t x; double value = x / 100.0;
0x23: int16_t x; double value = x / 1000.0;
0x30: int24_t value;
0x31: int24_t x; double value = x / 10.0;
0x32: int24_t x; double value = x / 100.0;
0x33: int24_t x; double value = x / 1000.0;
0x40: int32_t value;
0x41: int32_t x; double value = x / 10.0;
0x42: int32_t x; double value = x / 100.0;
0x43: int32_t x; double value = x / 1000.0;
0x50: int64_t value;
0x51: int64_t x; double value = x / 10.0;
0x52: int64_t x; double value = x / 100.0;
0x53: int64_t x; double value = x / 1000.0;
0x90: uint8_t value;
0x91: uint8_t x; double value = x / 10.0;
0x92: uint8_t x; double value = x / 100.0;
0x93: uint8_t x; double value = x / 1000.0;
0xa0: uint16_t value;
0xa1: uint16_t x; double value = x / 10.0;
0xa2: uint16_t x; double value = x / 100.0;
0xa3: uint16_t x; double value = x / 1000.0;
0xb0: uint24_t value;
0xb1: uint24_t x; double value = x / 10.0;
0xb2: uint24_t x; double value = x / 100.0;
0xb3: uint24_t x; double value = x / 1000.0;
0xc0: uint32_t value;
0xc1: uint32_t x; double value = x / 10.0;
0xc2: uint32_t x; double value = x / 100.0;
0xc3: uint32_t x; double value = x / 1000.0;
0xd0: uint64_t value;
0xd1: uint64_t x; double value = x / 10.0;
0xd2: uint64_t x; double value = x / 100.0;
0xd3: uint64_t x; double value = x / 1000.0;

LEB128
------
LEB128 is the usual encoding of unsigned integers of arbitrary size. Each byte encodes 7 bit of the integer, lowest bits first. All bytes except the last byte of such an encoding have bit 7 set.

ZigZag
------
ZigZag is the usual ZigZag encoding to encode signed integers of arbitrary size into an unsigned integer. The output of ZigZag is LEB128 encoded.

Constraints
-----------
- There must only be one channel definition per channel id.
- Channel ids are densely allocated. 16-bit channel ids are only used if there are > 240 channels in a file.
- Channel ids and definitions are only valid for the file they occur in. Different files use different channel ids for the same time series.
- A channel id X must be defined before any values for channel X occur in the sequence of entries.
- The initial timestamp must be set before the first value in the sequence of entries.
- A timestamp applies to all following values until a new timestamp is set.
