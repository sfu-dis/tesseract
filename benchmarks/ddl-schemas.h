#pragma once

#include "../engine.h"

struct Schema_base {
	uint64_t v;
};

struct Schema_record : public Schema_base {
	ermia::OrderedIndex* index;
	ermia::TableDescriptor *td;
};

struct Schema1 : public Schema_base {
	uint64_t a;
        uint64_t b;
};

struct Schema2 : public Schema_base {
	uint64_t a;
        uint64_t b;
        uint64_t c;
};
