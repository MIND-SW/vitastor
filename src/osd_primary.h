// Copyright (c) Vitaliy Filippov, 2019+
// License: VNPL-1.1 (see README.md for details)

#pragma once

#include "osd.h"
#include "osd_rmw.h"

#define SUBMIT_READ 0
#define SUBMIT_RMW_READ 1
#define SUBMIT_WRITE 2

struct unstable_osd_num_t
{
    osd_num_t osd_num;
    int start, len;
};

struct osd_primary_op_data_t
{
    int st = 0;
    pg_num_t pg_num;
    object_id oid;
    uint64_t target_ver;
    uint64_t orig_ver = 0, fact_ver = 0;
    uint64_t scheme = 0;
    int n_subops = 0, done = 0, errors = 0, epipe = 0;
    int degraded = 0, pg_size, pg_data_size;
    osd_rmw_stripe_t *stripes;
    osd_op_t *subops = NULL;
    uint64_t *prev_set = NULL;
    pg_osd_set_state_t *object_state = NULL;

    union
    {
        struct
        {
            // for sync. oops, requires freeing
            std::vector<unstable_osd_num_t> *unstable_write_osds;
            pool_pg_num_t *dirty_pgs;
            int dirty_pg_count;
            osd_num_t *dirty_osds;
            int dirty_osd_count;
            obj_ver_id *unstable_writes;
            obj_ver_osd_t *copies_to_delete;
            int copies_to_delete_count;
        };
        struct
        {
            // for read_bitmaps
            void *snapshot_bitmaps;
            inode_t *read_chain;
            uint8_t *missing_flags;
            int chain_size;
            osd_chain_read_t *chain_reads;
            int chain_read_count;
        };
    };
};

bool contains_osd(osd_num_t *osd_set, uint64_t size, osd_num_t osd_num);
