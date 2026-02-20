TimeSeriesDB file format
===================

TimeSeriesDB is a simple filesystem based database for time series data
like sensor data or PV inverter data.

Files
-----
One file per UTC day. New files start at 00:00:00 UTC. Each file contains multiple time series.
Filenames: data_2026-02-13.tsdb

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
uint8_t data[]; // Data in the format defined for this channel id (see type=0xf5).

uint8_t type = 0xff; // Escape for 16-bit (uint16_t) channel id. The 16 bit channel id must be defined by a type=0xf6 channel definition beforehand. Also the timestamp must be set beforehand.
uint16_t channelId16; // 16-bit channel id 0xf0..0xffff
uint8_t data[]; // Data in the format defined for this channel id (see type=0xf6).

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

SpecialEntries
--------------
uint8_t type = 0xfe; // End of file marker. The presence of this byte marks the file as complete. No more data will be appended.
                     // It is ok if this is missing. This indicates to a reader that a writer may still append data to this file at any time. This is usually the case for the current file for the current day.

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

Constraints
-----------
- There must only be one channel definition per channel id.
- Channel ids are densely allocated. 16-bit channel ids are only used if there are > 240 channels in a file.
- Channel ids and definitions are only valid for the file they occur in. Different files use different channel ids for the same time series.
- A channel id X must be defined before any values for channel X occur in the sequence of entries.
- The initial timestamp must be set before the first value in the sequence of entries.
- A timestamp applies to all following values until a new timestamp is set.
