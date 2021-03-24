#pragma once

struct TrunkLinkInfo {
    uint32_t trunk_id;
    uint32_t ref;
    uint8_t *data;
    TrunkLinkInfo *next;
};


class TrunkView {
public:
    uint32_t trunk_id;
    uint8_t* data;
    uint32_t length;
    TrunkLinkInfo *link_info;
    TrunkView(uint32_t trunk_id, uint8_t* data, uint32_t length, TrunkLinkInfo *info) : 
        trunk_id(trunk_id), data(data), length(length), link_info(info) {}
    TrunkView() : trunk_id(0), data(nullptr), length(0), link_info(NULL) {}
};
