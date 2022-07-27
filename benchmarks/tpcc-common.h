#ifndef ADV_COROUTINE

#include <ctype.h>
#include <sys/time.h>
#include <unistd.h>

#include <string>

#include "../catalog_mgr.h"
#include "bench.h"
#include "tpcc.h"

// configuration flags
extern int g_disable_xpartition_txn;
extern int g_enable_separate_tree_per_partition;
extern int g_new_order_remote_item_pct;
extern int g_new_order_fast_id_gen;
extern int g_uniform_item_dist;
extern int g_order_status_scan_hack;
extern int g_wh_temperature;
extern uint g_microbench_rows;  // this many rows
// can't have both ratio and rows at the same time
extern int g_microbench_wr_rows;  // this number of rows to write
extern int g_nr_suppliers;
extern int g_hybrid;
extern int ddl_example;

extern double g_wh_spread;

extern unsigned g_txn_workload_mix[8];

extern util::aligned_padded_elem<std::atomic<uint64_t>> *g_district_ids;

typedef std::vector<std::vector<std::pair<int32_t, int32_t>>> SuppStockMap;
extern SuppStockMap supp_stock_map;

// config constants
struct Nation {
  int id;
  std::string name;
  int rId;
};
extern const Nation nations[];

extern const char *regions[];

static constexpr ALWAYS_INLINE size_t NumItems() { return 100000; }

static constexpr ALWAYS_INLINE size_t NumDistrictsPerWarehouse() { return 10; }

static constexpr ALWAYS_INLINE size_t NumCustomersPerDistrict() { return 3000; }

static ALWAYS_INLINE size_t NumWarehouses() {
  return (size_t)ermia::config::benchmark_scale_factor;
}

static inline std::atomic<uint64_t> &NewOrderIdHolder(unsigned warehouse,
                                                      unsigned district) {
  ASSERT(warehouse >= 1 && warehouse <= NumWarehouses());
  ASSERT(district >= 1 && district <= NumDistrictsPerWarehouse());
  const unsigned idx =
      (warehouse - 1) * NumDistrictsPerWarehouse() + (district - 1);
  return g_district_ids[idx].elem;
}

static inline uint64_t FastNewOrderIdGen(unsigned warehouse,
                                         unsigned district) {
  return NewOrderIdHolder(warehouse, district)
      .fetch_add(1, std::memory_order_acq_rel);
}

struct eqstr {
  bool operator()(const char *s1, const char *s2) const {
    return (s1 == s2) || (s1 && s2 && strcmp(s1, s2) == 0);
  }
};

#ifndef NDEBUG
struct checker {
  // these sanity checks are just a few simple checks to make sure
  // the data is not entirely corrupted

  static ALWAYS_INLINE void SanityCheckCustomer(const customer::key *k,
                                                const customer::value *v) {
    ASSERT(v->c_credit == "BC" || v->c_credit == "GC");
    ASSERT(v->c_middle == "OE");
    ASSERT(k->c_w_id >= 1 && static_cast<size_t>(k->c_w_id) <= NumWarehouses());
    ASSERT(k->c_d_id >= 1 &&
           static_cast<size_t>(k->c_d_id) <= NumDistrictsPerWarehouse());
    ASSERT(k->c_id >= 1 &&
           static_cast<size_t>(k->c_id) <= NumCustomersPerDistrict());
  }

  static ALWAYS_INLINE void SanityCheckWarehouse(const warehouse::key *k,
                                                 const warehouse::value *v) {
    ASSERT(k->w_id >= 1 && static_cast<size_t>(k->w_id) <= NumWarehouses());
    ASSERT(v->w_state.size() == 2);
    ASSERT(v->w_zip == "123456789");
  }

  static ALWAYS_INLINE void SanityCheckDistrict(const district::key *k,
                                                const district::value *v) {
    ASSERT(k->d_w_id >= 1 && static_cast<size_t>(k->d_w_id) <= NumWarehouses());
    ASSERT(k->d_id >= 1 &&
           static_cast<size_t>(k->d_id) <= NumDistrictsPerWarehouse());
    ASSERT(v->d_next_o_id >= 3001);
    ASSERT(v->d_state.size() == 2);
    ASSERT(v->d_zip == "123456789");
  }

  static ALWAYS_INLINE void SanityCheckItem(const item::key *k,
                                            const item::value *v) {
    ASSERT(k->i_id >= 1 && static_cast<size_t>(k->i_id) <= NumItems());
    ASSERT(v->i_price >= 1.0 && v->i_price <= 100.0);
  }

  static ALWAYS_INLINE void SanityCheckStock(const stock::key *k) {
    ASSERT(k->s_w_id >= 1 && static_cast<size_t>(k->s_w_id) <= NumWarehouses());
    ASSERT(k->s_i_id >= 1 && static_cast<size_t>(k->s_i_id) <= NumItems());
  }

  static ALWAYS_INLINE void SanityCheckNewOrder(const new_order::key *k) {
    ASSERT(k->no_w_id >= 1 &&
           static_cast<size_t>(k->no_w_id) <= NumWarehouses());
    ASSERT(k->no_d_id >= 1 &&
           static_cast<size_t>(k->no_d_id) <= NumDistrictsPerWarehouse());
  }

  static ALWAYS_INLINE void SanityCheckOOrder(const oorder::key *k,
                                              const oorder::value *v) {
    ASSERT(k->o_w_id >= 1 && static_cast<size_t>(k->o_w_id) <= NumWarehouses());
    ASSERT(k->o_d_id >= 1 &&
           static_cast<size_t>(k->o_d_id) <= NumDistrictsPerWarehouse());
    ASSERT(v->o_c_id >= 1 &&
           static_cast<size_t>(v->o_c_id) <= NumCustomersPerDistrict());
    ASSERT(v->o_carrier_id >= 0 &&
           static_cast<size_t>(v->o_carrier_id) <= NumDistrictsPerWarehouse());
    ASSERT(v->o_ol_cnt >= 5 && v->o_ol_cnt <= 15);
  }

  static ALWAYS_INLINE void SanityCheckOrderLine(const order_line::key *k,
                                                 const order_line::value *v) {
    ASSERT(k->ol_w_id >= 1 &&
           static_cast<size_t>(k->ol_w_id) <= NumWarehouses());
    ASSERT(k->ol_d_id >= 1 &&
           static_cast<size_t>(k->ol_d_id) <= NumDistrictsPerWarehouse());
    ASSERT(k->ol_number >= 1 && k->ol_number <= 15);
    ASSERT(v->ol_i_id >= 1 && static_cast<size_t>(v->ol_i_id) <= NumItems());
  }
};
#endif

class tpcc_table_scanner : public ermia::OrderedIndex::ScanCallback {
 public:
  tpcc_table_scanner(ermia::str_arena *arena) : _arena(arena) {}
  virtual bool Invoke(const char *keyp, size_t keylen,
                      const ermia::varstr &value) {
    ermia::varstr *const k = _arena->next(keylen);
    ermia::varstr *v = _arena->next(0);  // header only
    v->p = value.p;
    v->l = value.l;
    ASSERT(k);
    k->copy_from(keyp, keylen);
    output.emplace_back(k, v);
    return true;
  }

  void clear() { output.clear(); }
  std::vector<std::pair<ermia::varstr *, const ermia::varstr *>> output;
  ermia::str_arena *_arena;
};

class tpcc_worker_mixin : private _dummy {
#define DEFN_TBL_INIT_X(name) , tbl_##name##_vec(partitions.at(#name))

 public:
  tpcc_worker_mixin(
      const std::map<std::string, std::vector<ermia::OrderedIndex *>>
          &partitions)
      : _dummy()  // so hacky...
        TPCC_TABLE_LIST(DEFN_TBL_INIT_X) {
    ALWAYS_ASSERT(NumWarehouses() >= 1);
  }

#undef DEFN_TBL_INIT_X

 protected:
#define DEFN_TBL_ACCESSOR_X(name)                                              \
 private:                                                                      \
  std::vector<ermia::OrderedIndex *> tbl_##name##_vec;                         \
                                                                               \
 protected:                                                                    \
  ALWAYS_INLINE ermia::ConcurrentMasstreeIndex *tbl_##name(unsigned int wid) { \
    ASSERT(wid >= 1 && wid <= NumWarehouses());                                \
    ASSERT(tbl_##name##_vec.size() == NumWarehouses());                        \
    return (ermia::ConcurrentMasstreeIndex *)tbl_##name##_vec[wid - 1];        \
  }

  TPCC_TABLE_LIST(DEFN_TBL_ACCESSOR_X)

#undef DEFN_TBL_ACCESSOR_X

 public:
  static inline uint32_t GetCurrentTimeMillis() {
    // struct timeval tv;
    // ALWAYS_ASSERT(gettimeofday(&tv, 0) == 0);
    // return tv.tv_sec * 1000;

    // XXX(stephentu): implement a scalable GetCurrentTimeMillis()
    // for now, we just give each core an increasing number

    static thread_local uint32_t tl_hack = 0;
    return tl_hack++;
  }

  // utils for generating random #s and strings

  static ALWAYS_INLINE int CheckBetweenInclusive(int v, int lower, int upper) {
    MARK_REFERENCED(lower);
    MARK_REFERENCED(upper);
    ASSERT(v >= lower);
    ASSERT(v <= upper);
    return v;
  }

  static ALWAYS_INLINE int RandomNumber(util::fast_random &r, int min,
                                        int max) {
    return CheckBetweenInclusive(
        (int)(r.next_uniform() * (max - min + 1) + min), min, max);
  }

  static ALWAYS_INLINE int NonUniformRandom(util::fast_random &r, int A, int C,
                                            int min, int max) {
    return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) %
            (max - min + 1)) +
           min;
  }

  static ALWAYS_INLINE int GetItemId(util::fast_random &r) {
    return CheckBetweenInclusive(
        g_uniform_item_dist ? RandomNumber(r, 1, NumItems())
                            : NonUniformRandom(r, 8191, 7911, 1, NumItems()),
        1, NumItems());
  }

  static ALWAYS_INLINE int GetCustomerId(util::fast_random &r) {
    return CheckBetweenInclusive(
        NonUniformRandom(r, 1023, 259, 1, NumCustomersPerDistrict()), 1,
        NumCustomersPerDistrict());
  }

  static std::string NameTokens[];

  // all tokens are at most 5 chars long
  static const size_t CustomerLastNameMaxSize = 5 * 3;

  static inline size_t GetCustomerLastName(uint8_t *buf, int num) {
    const std::string &s0 = NameTokens[num / 100];
    const std::string &s1 = NameTokens[(num / 10) % 10];
    const std::string &s2 = NameTokens[num % 10];
    uint8_t *const begin = buf;
    const size_t s0_sz = s0.size();
    const size_t s1_sz = s1.size();
    const size_t s2_sz = s2.size();
    memcpy(buf, s0.data(), s0_sz);
    buf += s0_sz;
    memcpy(buf, s1.data(), s1_sz);
    buf += s1_sz;
    memcpy(buf, s2.data(), s2_sz);
    buf += s2_sz;
    return buf - begin;
  }

  static inline std::string GetCustomerLastName(int num) {
    std::string ret;
    ret.resize(CustomerLastNameMaxSize);
    ret.resize(GetCustomerLastName((uint8_t *)&ret[0], num));
    return ret;
  }

  static ALWAYS_INLINE std::string GetNonUniformCustomerLastNameLoad(
      util::fast_random &r) {
    return GetCustomerLastName(NonUniformRandom(r, 255, 157, 0, 999));
  }

  static ALWAYS_INLINE size_t
  GetNonUniformCustomerLastNameRun(uint8_t *buf, util::fast_random &r) {
    return GetCustomerLastName(buf, NonUniformRandom(r, 255, 223, 0, 999));
  }

  static ALWAYS_INLINE size_t
  GetNonUniformCustomerLastNameRun(char *buf, util::fast_random &r) {
    return GetNonUniformCustomerLastNameRun((uint8_t *)buf, r);
  }

  static ALWAYS_INLINE std::string GetNonUniformCustomerLastNameRun(
      util::fast_random &r) {
    return GetCustomerLastName(NonUniformRandom(r, 255, 223, 0, 999));
  }

  // following oltpbench, we really generate strings of len - 1...
  static inline std::string RandomStr(util::fast_random &r, uint len) {
    // this is a property of the oltpbench implementation...
    if (!len) return "";

    uint i = 0;
    std::string buf(len - 1, 0);
    while (i < (len - 1)) {
      const char c = (char)r.next_char();
      // XXX(stephentu): oltpbench uses java's Character.isLetter(), which
      // is a less restrictive filter than isalnum()
      if (!isalnum(c)) continue;
      buf[i++] = c;
    }
    return buf;
  }

  // RandomNStr() actually produces a std::string of length len
  static inline std::string RandomNStr(util::fast_random &r, uint len) {
    const char base = '0';
    std::string buf(len, 0);
    for (uint i = 0; i < len; i++) buf[i] = (char)(base + (r.next() % 10));
    return buf;
  }

  // 80/20 access: 80% of all accesses touch 20% of WHs (randmonly
  // choose one from hot_whs), while the 20% of accesses touch the
  // remaining 80% of WHs.
  static std::vector<uint> hot_whs;
  static std::vector<uint> cold_whs;

  ALWAYS_INLINE unsigned pick_wh(util::fast_random &r, uint home_wh) {
    if (g_wh_temperature) {  // do it 80/20 way
      uint w = 0;
      if (r.next_uniform() >= 0.2)  // 80% access
        w = hot_whs[r.next() % hot_whs.size()];
      else
        w = cold_whs[r.next() % cold_whs.size()];
      LOG_IF(FATAL, w < 1 || w > NumWarehouses());
      return w;
    } else {
      ASSERT(g_wh_spread >= 0 and g_wh_spread <= 1);
      // wh_spread = 0: always use home wh
      // wh_spread = 1: always use random wh
      if (g_wh_spread == 0 || r.next_uniform() >= g_wh_spread) return home_wh;
      return r.next() % NumWarehouses() + 1;
    }
  }
};

class tpcc_schematable_loader : public ermia::catalog::schematable_loader {
 public:
  tpcc_schematable_loader(
      unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables)
      : ermia::catalog::schematable_loader(seed, db, open_tables) {}

 protected:
  void load() {
    ermia::OrderedIndex *tbl = open_tables.at("SCHEMA");
    ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());

    auto add_column = [=](ermia::varstr *key, ermia::varstr &value,
                          ermia::str_arena *arena, uint64_t schema_version,
                          ermia::FID fid, ermia::OID oid, ermia::transaction *t,
                          uint64_t begin, bool insert) {
      order_line::value v_ol_temp;
      const order_line::value *v_ol = Decode(value, v_ol_temp);

      order_line_1::value v_ol_1;
      v_ol_1.ol_i_id = v_ol->ol_i_id;
      v_ol_1.ol_delivery_d = v_ol->ol_delivery_d;
      v_ol_1.ol_amount = v_ol->ol_amount;
      v_ol_1.ol_supply_w_id = v_ol->ol_supply_w_id;
      v_ol_1.ol_quantity = v_ol->ol_quantity;
      v_ol_1.ol_tax = (float)0.1;

      const size_t order_line_sz = ::Size(v_ol_1);
      ermia::varstr *new_value = arena->next(order_line_sz);
      new_value = &Encode(*new_value, v_ol_1);

      return new_value;
    };

    const schema_kv::key k1(
        ermia::Catalog::GetTable("order_line")->GetTupleFid());
    const schema_kv::key k2(ermia::Catalog::GetTable("oorder")->GetTupleFid());
    const schema_kv::key k3(
        ermia::Catalog::GetTable("customer")->GetTupleFid());

    struct ermia::schema_record order_line_schema;
    order_line_schema.state = ermia::ddl::schema_state_type::COMPLETE;
    order_line_schema.old_td = nullptr;
    order_line_schema.old_index = nullptr;

    struct ermia::schema_record oorder_schema;
    oorder_schema.state = ermia::ddl::schema_state_type::COMPLETE;
    oorder_schema.old_td = nullptr;
    oorder_schema.old_index = nullptr;

    struct ermia::schema_record customer_schema;
    customer_schema.state = ermia::ddl::schema_state_type::COMPLETE;
    customer_schema.old_td = nullptr;
    customer_schema.old_index = nullptr;

    order_line_schema.v = 0;
    order_line_schema.csn = 0;
    order_line_schema.reformat_idx = ermia::ddl::reformats.size();
    order_line_schema.reformats[0] = ermia::ddl::reformats.size();
    ermia::ddl::reformats.push_back(add_column);
    order_line_schema.secondary_index_key_create_idx = -1;
    order_line_schema.constraint_idx = -1;
    order_line_schema.td = ermia::Catalog::GetTable("order_line");
    order_line_schema.index = order_line_schema.td->GetPrimaryIndex();
    order_line_schema.show_index = ddl_examples[0] == 3 ? false : true;
    order_line_schema.write_key = (ddl_examples[0] == 0 || ddl_examples[0] == 2 ||
        ddl_examples[0] == 3 || ddl_examples[0] == 4 || ddl_examples[0] == 9 ||
        ddl_examples[0] == 10) ? true : false;
    order_line_schema.reformats_total = 0;
    order_line_schema.old_tds_total = 0;
    schema_kv::value order_line_schema_value;
    order_line_schema.record_to_value(order_line_schema_value);

    oorder_schema.v = 0;
    oorder_schema.csn = 0;
    oorder_schema.secondary_index_key_create_idx = -1;
    oorder_schema.td = ermia::Catalog::GetTable("oorder");
    oorder_schema.index = oorder_schema.td->GetPrimaryIndex();
    oorder_schema.show_index = true;
    oorder_schema.write_key = (ddl_examples[0] == 2) ? true : false;
    oorder_schema.reformats_total = 0;
    oorder_schema.old_tds_total = 0;
    schema_kv::value oorder_schema_value;
    oorder_schema.record_to_value(oorder_schema_value);

    customer_schema.v = 0;
    customer_schema.csn = 0;
    customer_schema.reformat_idx = -1;
    customer_schema.constraint_idx = -1;
    customer_schema.secondary_index_key_create_idx = -1;
    customer_schema.td = ermia::Catalog::GetTable("customer");
    customer_schema.index = customer_schema.td->GetPrimaryIndex();
    customer_schema.show_index = true;
    customer_schema.write_key = (ddl_examples[0] == 1) ? true : false;
    customer_schema.reformats_total = 0;
    customer_schema.old_tds_total = 0;
    schema_kv::value customer_schema_value;
    customer_schema.record_to_value(customer_schema_value);

    TryVerifyStrict(tbl->InsertRecord(
        txn, Encode(str(Size(k1)), k1),
        Encode(str(Size(order_line_schema_value)), order_line_schema_value)));
    TryVerifyStrict(tbl->InsertRecord(
        txn, Encode(str(Size(k2)), k2),
        Encode(str(Size(oorder_schema_value)), oorder_schema_value)));
    TryVerifyStrict(tbl->InsertRecord(
        txn, Encode(str(Size(k3)), k3),
        Encode(str(Size(customer_schema_value)), customer_schema_value)));
    TryVerifyStrict(db->Commit(txn));

    if (ermia::config::verbose) {
      std::cerr << "[INFO] schema table loaded" << std::endl;
    };
  }
};

class tpcc_nation_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_nation_loader(
      unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      const std::map<std::string, std::vector<ermia::OrderedIndex *>>
          &partitions)
      : bench_loader(seed, db, open_tables), tpcc_worker_mixin(partitions) {}

 protected:
  virtual void load() {
    std::string obj_buf;
    ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
    uint i;
    uint64_t total_sz = 0;
    for (i = 0; i < 62; i++) {
      const nation::key k(nations[i].id);
      nation::value v;
      total_sz += Size(v);

      const std::string n_comment = RandomStr(r, RandomNumber(r, 10, 20));
      v.n_name = std::string(nations[i].name);
      v.n_regionkey = nations[i].rId;
      v.n_comment.assign(n_comment);
      TryVerifyStrict(tbl_nation(1)->InsertRecord(txn, Encode(str(Size(k)), k),
                                                  Encode(str(Size(v)), v)));
    }
    TryVerifyStrict(db->Commit(txn));
    LOG(INFO) << "Finished loading nation";
    LOG(INFO) << "  * total/average nation record length: " << total_sz << "/"
              << (double(total_sz) / double(62)) << " bytes";
  }
};

class tpcc_region_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_region_loader(
      unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      const std::map<std::string, std::vector<ermia::OrderedIndex *>>
          &partitions)
      : bench_loader(seed, db, open_tables), tpcc_worker_mixin(partitions) {}

 protected:
  virtual void load() {
    uint64_t total_sz = 0;
    std::string obj_buf;
    ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
    for (uint i = 0; i < 5; i++) {
      const region::key k(i);
      region::value v;

      v.r_name = std::string(regions[i]);
      const std::string r_comment = RandomStr(r, RandomNumber(r, 10, 20));
      v.r_comment.assign(r_comment);
      TryVerifyStrict(tbl_region(1)->InsertRecord(txn, Encode(str(Size(k)), k),
                                                  Encode(str(Size(v)), v)));
      total_sz += Size(v);
    }
    TryVerifyStrict(db->Commit(txn));
    LOG(INFO) << "Finished loading region";
    LOG(INFO) << "  * total/average region record length: " << total_sz << "/"
              << (double(total_sz) / double(5)) << " bytes";
  }
};

class tpcc_supplier_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_supplier_loader(
      unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      const std::map<std::string, std::vector<ermia::OrderedIndex *>>
          &partitions)
      : bench_loader(seed, db, open_tables), tpcc_worker_mixin(partitions) {}

 protected:
  virtual void load() {
    uint64_t total_sz = 0;
    std::string obj_buf;
    for (uint i = 0; i < 10000; i++) {
      ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
      const supplier::key k(i);
      supplier::value v;

      v.su_name = std::string("Supplier#") + std::string("000000000") +
                  std::to_string(i);
      v.su_address = RandomStr(r, RandomNumber(r, 10, 40));

      auto rand = 0;
      while (rand == 0 || (rand > '9' && rand < 'A') ||
             (rand > 'Z' && rand < 'a'))
        rand = RandomNumber(r, '0', 'z');
      v.su_nationkey = rand;
      //		  v.su_phone = std::string("911"); //
      // XXX. nobody wants this field
      //		  v.su_acctbal = 0;
      //		  v.su_comment = RandomStr(r, RandomNumber(r,10,39));
      //// XXX. Q16 uses this. fix this if needed.

      TryVerifyStrict(tbl_supplier(1)->InsertRecord(
          txn, Encode(str(Size(k)), k), Encode(str(Size(v)), v)));

      TryVerifyStrict(db->Commit(txn));
      total_sz += Size(v);
    }
    LOG(INFO) << "Finished loading supplier";
    LOG(INFO) << "  * total/average supplier record length: " << total_sz << "/"
              << (double(total_sz) / double(10000)) << " bytes";
  }
};

class tpcc_warehouse_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_warehouse_loader(
      unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      const std::map<std::string, std::vector<ermia::OrderedIndex *>>
          &partitions)
      : bench_loader(seed, db, open_tables), tpcc_worker_mixin(partitions) {}

 protected:
  virtual void load() {
    std::string obj_buf;
    uint64_t warehouse_total_sz = 0, n_warehouses = 0;
    std::vector<warehouse::value> warehouses;
    for (uint i = 1; i <= NumWarehouses(); i++) {
      arena->reset();
      ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
      const warehouse::key k(i);

      const std::string w_name = RandomStr(r, RandomNumber(r, 6, 10));
      const std::string w_street_1 = RandomStr(r, RandomNumber(r, 10, 20));
      const std::string w_street_2 = RandomStr(r, RandomNumber(r, 10, 20));
      const std::string w_city = RandomStr(r, RandomNumber(r, 10, 20));
      const std::string w_state = RandomStr(r, 3);
      const std::string w_zip = "123456789";

      warehouse::value v;
      v.w_ytd = 300000;
      v.w_tax = (float)RandomNumber(r, 0, 2000) / 10000.0;
      v.w_name.assign(w_name);
      v.w_street_1.assign(w_street_1);
      v.w_street_2.assign(w_street_2);
      v.w_city.assign(w_city);
      v.w_state.assign(w_state);
      v.w_zip.assign(w_zip);

#ifndef NDEBUG
      checker::SanityCheckWarehouse(&k, &v);
#endif
      const size_t sz = Size(v);
      warehouse_total_sz += sz;
      n_warehouses++;
      TryVerifyStrict(tbl_warehouse(i)->InsertRecord(
          txn, Encode(str(Size(k)), k), Encode(str(sz), v)));

      warehouses.push_back(v);
      TryVerifyStrict(db->Commit(txn));
    }
    for (uint i = 1; i <= NumWarehouses(); i++) {
      arena->reset();
      ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
      const warehouse::key k(i);
      warehouse::value warehouse_temp;
      ermia::varstr warehouse_v;

      rc_t rc = rc_t{RC_INVALID};
      tbl_warehouse(i)->GetRecord(txn, rc, Encode(str(Size(k)), k),
                                  warehouse_v);
      TryVerifyStrict(rc);

      const warehouse::value *v = Decode(warehouse_v, warehouse_temp);
      ALWAYS_ASSERT(warehouses[i - 1] == *v);

#ifndef NDEBUG
      checker::SanityCheckWarehouse(&k, v);
#endif
      TryVerifyStrict(db->Commit(txn));
    }

    // pre-build supp-stock mapping table to boost tpc-ch queries
    for (uint w = 1; w <= NumWarehouses(); w++) {
      for (uint i = 1; i <= NumItems(); i++) {
        supp_stock_map[w * i % 10000].push_back(std::make_pair(w, i));
      }
    }
    LOG(INFO) << "Finished loading warehouse";
    LOG(INFO) << "  * total/average warehouse record length: "
              << warehouse_total_sz << "/"
              << (double(warehouse_total_sz) / double(n_warehouses))
              << " bytes";
  }
};

class tpcc_item_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_item_loader(
      unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      const std::map<std::string, std::vector<ermia::OrderedIndex *>>
          &partitions)
      : bench_loader(seed, db, open_tables), tpcc_worker_mixin(partitions) {}

 protected:
  virtual void load() {
    std::string obj_buf;
    uint64_t total_sz = 0;
    for (uint i = 1; i <= NumItems(); i++) {
      arena->reset();
      ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
      // items don't "belong" to a certain warehouse, so no pinning
      const item::key k(i);

      item::value v;
      const std::string i_name = RandomStr(r, RandomNumber(r, 14, 24));
      v.i_name.assign(i_name);
      v.i_price = (float)RandomNumber(r, 100, 10000) / 100.0;
      const int len = RandomNumber(r, 26, 50);
      if (RandomNumber(r, 1, 100) > 10) {
        const std::string i_data = RandomStr(r, len);
        v.i_data.assign(i_data);
      } else {
        const int startOriginal = RandomNumber(r, 2, (len - 8));
        const std::string i_data = RandomStr(r, startOriginal + 1) +
                                   "ORIGINAL" +
                                   RandomStr(r, len - startOriginal - 7);
        v.i_data.assign(i_data);
      }
      v.i_im_id = RandomNumber(r, 1, 10000);

#ifndef NDEBUG
      checker::SanityCheckItem(&k, &v);
#endif
      const size_t sz = Size(v);
      total_sz += sz;
      TryVerifyStrict(tbl_item(1)->InsertRecord(
          txn, Encode(str(Size(k)), k),
          Encode(str(sz), v)));  // this table is shared, so any partition is OK
      TryVerifyStrict(db->Commit(txn));
    }
    if (ermia::config::verbose) {
      LOG(INFO) << "Finished loading item";
      LOG(INFO) << "  * total/average item record length: " << total_sz << "/"
                << (double(total_sz) / double(NumItems())) << " bytes";
    }
  }
};

class tpcc_stock_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_stock_loader(
      unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      const std::map<std::string, std::vector<ermia::OrderedIndex *>>
          &partitions,
      ssize_t warehouse_id)
      : bench_loader(seed, db, open_tables),
        tpcc_worker_mixin(partitions),
        warehouse_id(warehouse_id) {
    ALWAYS_ASSERT(warehouse_id == -1 ||
                  (warehouse_id >= 1 &&
                   static_cast<size_t>(warehouse_id) <= NumWarehouses()));
  }

 protected:
  virtual void load() {
    std::string obj_buf, obj_buf1;

    uint64_t stock_total_sz = 0, n_stocks = 0;
    const uint w_start =
        (warehouse_id == -1) ? 1 : static_cast<uint>(warehouse_id);
    const uint w_end = (warehouse_id == -1) ? NumWarehouses()
                                            : static_cast<uint>(warehouse_id);

    ermia::schema_record schema;
    schema.write_key = true;
    schema.td = tbl_stock(1)->GetTableDescriptor();
    for (uint w = w_start; w <= w_end; w++) {
      const size_t batchsize = 10;
      for (size_t i = 0; i < NumItems();) {
        size_t iend = std::min(i + batchsize, NumItems());
        ermia::scoped_str_arena s_arena(*arena);
        for (uint j = i + 1; j <= iend; j++) {
          arena->reset();
          ermia::transaction *const txn =
              db->NewTransaction(0, *arena, txn_buf());
          const stock::key k(w, j);
          const stock_data::key k_data(w, j);

          stock::value v;
          v.s_quantity = RandomNumber(r, 10, 100);
          v.s_ytd = 0;
          v.s_order_cnt = 0;
          v.s_remote_cnt = 0;

          stock_data::value v_data;
          const int len = RandomNumber(r, 26, 50);
          if (RandomNumber(r, 1, 100) > 10) {
            const std::string s_data = RandomStr(r, len);
            v_data.s_data.assign(s_data);
          } else {
            const int startOriginal = RandomNumber(r, 2, (len - 8));
            const std::string s_data = RandomStr(r, startOriginal + 1) +
                                       "ORIGINAL" +
                                       RandomStr(r, len - startOriginal - 7);
            v_data.s_data.assign(s_data);
          }
          v_data.s_dist_01.assign(RandomStr(r, 24));
          v_data.s_dist_02.assign(RandomStr(r, 24));
          v_data.s_dist_03.assign(RandomStr(r, 24));
          v_data.s_dist_04.assign(RandomStr(r, 24));
          v_data.s_dist_05.assign(RandomStr(r, 24));
          v_data.s_dist_06.assign(RandomStr(r, 24));
          v_data.s_dist_07.assign(RandomStr(r, 24));
          v_data.s_dist_08.assign(RandomStr(r, 24));
          v_data.s_dist_09.assign(RandomStr(r, 24));
          v_data.s_dist_10.assign(RandomStr(r, 24));

#ifndef NDEBUG
          checker::SanityCheckStock(&k);
#endif
          const size_t sz = Size(v);
          stock_total_sz += sz;
          n_stocks++;
          TryVerifyStrict(tbl_stock(w)->InsertRecord(
              txn, Encode(str(Size(k)), k), Encode(str(sz), v), nullptr, &schema));
          TryVerifyStrict(tbl_stock_data(w)->InsertRecord(
              txn, Encode(str(Size(k_data)), k_data),
              Encode(str(Size(v_data)), v_data)));
          TryVerifyStrict(db->Commit(txn));
        }

        // loop update
        i = iend;
      }
    }
    if (warehouse_id == -1) {
      LOG(INFO) << "Finished loading stock";
      LOG(INFO) << "  * total/average stock record length: " << stock_total_sz
                << "/" << (double(stock_total_sz) / double(n_stocks))
                << " bytes";
    } else {
      LOG(INFO) << "Finished loading stock (w=" << warehouse_id << ")";
      LOG(INFO) << "  * total/average stock (w=" << warehouse_id
                << ") record length: " << stock_total_sz << "/"
                << (double(stock_total_sz) / double(n_stocks)) << " bytes";
    }
  }

 private:
  ssize_t warehouse_id;
};

class tpcc_district_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_district_loader(
      unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      const std::map<std::string, std::vector<ermia::OrderedIndex *>>
          &partitions)
      : bench_loader(seed, db, open_tables), tpcc_worker_mixin(partitions) {}

 protected:
  virtual void load() {
    std::string obj_buf;

    const ssize_t bsize = 10;
    uint64_t district_total_sz = 0, n_districts = 0;
    uint cnt = 0;
    for (uint w = 1; w <= NumWarehouses(); w++) {
      for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++, cnt++) {
        arena->reset();
        ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
        const district::key k(w, d);

        district::value v;
        v.d_ytd = 30000;
        v.d_tax = (float)(RandomNumber(r, 0, 2000) / 10000.0);
        v.d_next_o_id = 3001;
        v.d_name.assign(RandomStr(r, RandomNumber(r, 6, 10)));
        v.d_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
        v.d_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
        v.d_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
        v.d_state.assign(RandomStr(r, 3));
        v.d_zip.assign("123456789");

#ifndef NDEBUG
        checker::SanityCheckDistrict(&k, &v);
#endif
        const size_t sz = Size(v);
        district_total_sz += sz;
        n_districts++;
        TryVerifyStrict(tbl_district(w)->InsertRecord(
            txn, Encode(str(Size(k)), k), Encode(str(sz), v)));

        TryVerifyStrict(db->Commit(txn));
      }
    }
    if (ermia::config::verbose) {
      LOG(INFO) << "Finished loading district";
      LOG(INFO) << "   * total/average district record length: "
                << district_total_sz << "/"
                << (double(district_total_sz) / double(n_districts))
                << " bytes";
    }
  }
};

class tpcc_customer_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_customer_loader(
      unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      const std::map<std::string, std::vector<ermia::OrderedIndex *>>
          &partitions,
      ssize_t warehouse_id)
      : bench_loader(seed, db, open_tables),
        tpcc_worker_mixin(partitions),
        warehouse_id(warehouse_id) {
    ALWAYS_ASSERT(warehouse_id == -1 ||
                  (warehouse_id >= 1 &&
                   static_cast<size_t>(warehouse_id) <= NumWarehouses()));
  }

 protected:
  virtual void load() {
    std::string obj_buf;

    const uint w_start =
        (warehouse_id == -1) ? 1 : static_cast<uint>(warehouse_id);
    const uint w_end = (warehouse_id == -1) ? NumWarehouses()
                                            : static_cast<uint>(warehouse_id);
    const size_t batchsize = 10;
    const size_t nbatches = (batchsize > NumCustomersPerDistrict())
                                ? 1
                                : (NumCustomersPerDistrict() / batchsize);

    uint64_t total_sz = 0;

    ermia::schema_record schema;
    schema.write_key = true;
    schema.td = tbl_customer(1)->GetTableDescriptor();
    for (uint w = w_start; w <= w_end; w++) {
      for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
        for (uint batch = 0; batch < nbatches;) {
          const size_t cstart = batch * batchsize;
          const size_t cend =
              std::min((batch + 1) * batchsize, NumCustomersPerDistrict());
          for (uint cidx0 = cstart; cidx0 < cend; cidx0++) {
            ermia::scoped_str_arena s_arena(arena);
            arena->reset();
            ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
            const uint c = cidx0 + 1;
            const customer::key k(w, d, c);

            customer::value v;
            v.c_id = c;  // Put the c_id here in the tuple, needed by
                         // order-status later
            v.c_discount = (float)(RandomNumber(r, 1, 5000) / 10000.0);
            if (RandomNumber(r, 1, 100) <= 10)
              v.c_credit.assign("BC");
            else
              v.c_credit.assign("GC");

            if (c <= 1000)
              v.c_last.assign(GetCustomerLastName(c - 1));
            else
              v.c_last.assign(GetNonUniformCustomerLastNameLoad(r));

            v.c_first.assign(RandomStr(r, RandomNumber(r, 8, 16)));
            v.c_credit_lim = 50000;

            v.c_balance = -10;
            v.c_ytd_payment = 10;
            v.c_payment_cnt = 1;
            v.c_delivery_cnt = 0;

            v.c_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
            v.c_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
            v.c_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
            v.c_state.assign(RandomStr(r, 3));
            v.c_zip.assign(RandomNStr(r, 4) + "11111");
            v.c_phone.assign(RandomNStr(r, 16));
            v.c_since = GetCurrentTimeMillis();
            v.c_middle.assign("OE");
            v.c_data.assign(RandomStr(r, RandomNumber(r, 300, 500)));

            v.v = 0;

#ifndef NDEBUG
            checker::SanityCheckCustomer(&k, &v);
#endif
            const size_t sz = Size(v);
            total_sz += sz;
            ermia::OID c_oid =
                0;  // Get the OID and put in customer_name_idx later
            TryVerifyStrict(tbl_customer(w)->InsertRecord(
                txn, Encode(str(Size(k)), k), Encode(str(sz), v), &c_oid, &schema));
            TryVerifyStrict(db->Commit(txn));

            // customer name index
            const customer_name_idx::key k_idx(
                k.c_w_id, k.c_d_id, v.c_last.str(true), v.c_first.str(true));

            // index structure is:
            // (c_w_id, c_d_id, c_last, c_first) -> OID

            arena->reset();
            txn = db->NewTransaction(0, *arena, txn_buf());
            TryVerifyStrict(tbl_customer_name_idx(w)->InsertOID(
                txn, Encode(str(Size(k_idx)), k_idx), c_oid));
            TryVerifyStrict(db->Commit(txn));
            arena->reset();

            history::key k_hist;
            k_hist.h_c_id = c;
            k_hist.h_c_d_id = d;
            k_hist.h_c_w_id = w;
            k_hist.h_d_id = d;
            k_hist.h_w_id = w;
            k_hist.h_date = GetCurrentTimeMillis();

            history::value v_hist;
            v_hist.h_amount = 10;
            v_hist.h_data.assign(RandomStr(r, RandomNumber(r, 10, 24)));

            arena->reset();
            txn = db->NewTransaction(0, *arena, txn_buf());
            TryVerifyStrict(tbl_history(w)->InsertRecord(
                txn, Encode(str(Size(k_hist)), k_hist),
                Encode(str(Size(v_hist)), v_hist)));
            TryVerifyStrict(db->Commit(txn));
          }
          batch++;
        }
      }
    }
    if (warehouse_id == -1) {
      LOG(INFO) << "Finished loading customer";
      LOG(INFO) << "   * total/average customer record length: " << total_sz
                << "/"
                << (double(total_sz) /
                    double(NumWarehouses() * NumDistrictsPerWarehouse() *
                           NumCustomersPerDistrict()))
                << " bytes ";
    } else {
      LOG(INFO) << "Finished loading customer (w=" << warehouse_id << ")";
      LOG(INFO) << "   * total/average customer (w=" << warehouse_id
                << ") record length: " << total_sz << "/"
                << (double(total_sz) /
                    double(NumWarehouses() * NumDistrictsPerWarehouse() *
                           NumCustomersPerDistrict()))
                << " bytes ";
    }
  }

 private:
  ssize_t warehouse_id;
};

class tpcc_order_loader : public bench_loader, public tpcc_worker_mixin {
 public:
  tpcc_order_loader(
      unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      const std::map<std::string, std::vector<ermia::OrderedIndex *>>
          &partitions,
      ssize_t warehouse_id)
      : bench_loader(seed, db, open_tables),
        tpcc_worker_mixin(partitions),
        warehouse_id(warehouse_id) {
    ALWAYS_ASSERT(warehouse_id == -1 ||
                  (warehouse_id >= 1 &&
                   static_cast<size_t>(warehouse_id) <= NumWarehouses()));
  }

 protected:
  size_t NumOrderLinesPerCustomer() { return RandomNumber(r, 5, 15); }

  virtual void load() {
    std::string obj_buf;

    uint64_t order_line_total_sz = 0, n_order_lines = 0;
    uint64_t oorder_total_sz = 0, n_oorders = 0;
    uint64_t new_order_total_sz = 0, n_new_orders = 0;

    const uint w_start =
        (warehouse_id == -1) ? 1 : static_cast<uint>(warehouse_id);
    const uint w_end = (warehouse_id == -1) ? NumWarehouses()
                                            : static_cast<uint>(warehouse_id);

    ermia::schema_record schema, schema1;
    schema.write_key = true;
    schema1.write_key = true;
    schema.td = tbl_oorder(1)->GetTableDescriptor();
    schema1.td = tbl_order_line(1)->GetTableDescriptor();
    for (uint w = w_start; w <= w_end; w++) {
      for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
        std::set<uint> c_ids_s;
        std::vector<uint> c_ids;
        while (c_ids.size() != NumCustomersPerDistrict()) {
          const auto x = (r.next() % NumCustomersPerDistrict()) + 1;
          if (c_ids_s.count(x)) continue;
          c_ids_s.insert(x);
          c_ids.emplace_back(x);
        }
        for (uint c = 1; c <= NumCustomersPerDistrict();) {
          ermia::scoped_str_arena s_arena(arena);
          arena->reset();
          ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
          const oorder::key k_oo(w, d, c);

          oorder::value v_oo;
          v_oo.o_c_id = c_ids[c - 1];
          if (k_oo.o_id < 2101)
            v_oo.o_carrier_id = RandomNumber(r, 1, 10);
          else
            v_oo.o_carrier_id = 0;
          v_oo.o_ol_cnt = NumOrderLinesPerCustomer();
          v_oo.o_all_local = 1;
          v_oo.o_entry_d = GetCurrentTimeMillis();

#ifndef NDEBUG
          checker::SanityCheckOOrder(&k_oo, &v_oo);
#endif
          const size_t sz = Size(v_oo);
          oorder_total_sz += sz;
          n_oorders++;
          ermia::OID v_oo_oid =
              0;  // Get the OID and put it in oorder_c_id_idx later
          TryVerifyStrict(
              tbl_oorder(w)->InsertRecord(txn, Encode(str(Size(k_oo)), k_oo),
                                          Encode(str(sz), v_oo), &v_oo_oid, &schema));
          TryVerifyStrict(db->Commit(txn));
          arena->reset();
          txn = db->NewTransaction(0, *arena, txn_buf());

          const oorder_c_id_idx::key k_oo_idx(k_oo.o_w_id, k_oo.o_d_id,
                                              v_oo.o_c_id, k_oo.o_id);
          TryVerifyStrict(tbl_oorder_c_id_idx(w)->InsertOID(
              txn, Encode(str(Size(k_oo_idx)), k_oo_idx), v_oo_oid));
          TryVerifyStrict(db->Commit(txn));

          if (c >= 2101) {
            arena->reset();
            txn = db->NewTransaction(0, *arena, txn_buf());
            const new_order::key k_no(w, d, c);
            const new_order::value v_no;

#ifndef NDEBUG
            checker::SanityCheckNewOrder(&k_no);
#endif
            const size_t sz = Size(v_no);
            new_order_total_sz += sz;
            n_new_orders++;
            TryVerifyStrict(tbl_new_order(w)->InsertRecord(
                txn, Encode(str(Size(k_no)), k_no), Encode(str(sz), v_no)));
            TryVerifyStrict(db->Commit(txn));
          }

          for (uint l = 1; l <= uint(v_oo.o_ol_cnt); l++) {
            const order_line::key k_ol(w, d, c, l);

            order_line::value v_ol;
            v_ol.ol_i_id = RandomNumber(r, 1, 100000);
            if (k_ol.ol_o_id < 2101) {
              v_ol.ol_delivery_d = v_oo.o_entry_d;
              v_ol.ol_amount = 0;
            } else {
              v_ol.ol_delivery_d = 0;
              // random within [0.01 .. 9,999.99]
              v_ol.ol_amount = (float)(RandomNumber(r, 1, 999999) / 100.0);
            }

            v_ol.ol_supply_w_id = k_ol.ol_w_id;
            v_ol.ol_quantity = 5;
            // v_ol.ol_dist_info comes from stock_data(ol_supply_w_id, ol_o_id)
            // v_ol.ol_dist_info = RandomStr(r, 24);

#ifndef NDEBUG
            checker::SanityCheckOrderLine(&k_ol, &v_ol);
#endif
            const size_t sz = Size(v_ol);
            order_line_total_sz += sz;
            n_order_lines++;
            arena->reset();
            txn = db->NewTransaction(0, *arena, txn_buf());
            TryVerifyStrict(tbl_order_line(w)->InsertRecord(
                txn, Encode(str(Size(k_ol)), k_ol), Encode(str(sz), v_ol), nullptr, &schema1));
            TryVerifyStrict(db->Commit(txn));
          }
          c++;
        }
      }
    }

    if (ermia::config::verbose) {
      if (warehouse_id == -1) {
        LOG(INFO) << "finished loading order";
        LOG(INFO) << "  * average order_line record length: "
                  << (double(order_line_total_sz) / double(n_order_lines))
                  << " bytes";
        LOG(INFO) << "  * average oorder record length: "
                  << (double(oorder_total_sz) / double(n_oorders)) << " bytes";
        LOG(INFO) << "   * average new_order record length: "
                  << (double(new_order_total_sz) / double(n_new_orders))
                  << " bytes";
      } else {
        LOG(INFO) << " Finished loading order (w=" << warehouse_id << ")";
        LOG(INFO) << "  * total/average order_line (w=" << warehouse_id
                  << ") record length: " << order_line_total_sz << "/"
                  << (double(order_line_total_sz) / double(n_order_lines))
                  << " bytes";
        LOG(INFO) << "  * total/average oorder record length: "
                  << oorder_total_sz << "/"
                  << (double(oorder_total_sz) / double(n_oorders)) << " bytes";
        LOG(INFO) << "   * total/average new_order record length: "
                  << new_order_total_sz << "/"
                  << (double(new_order_total_sz) / double(n_new_orders))
                  << " bytes";
      }
    }
  }

 private:
  ssize_t warehouse_id;
};

// explicitly copies keys, because btree::search_range_call() interally
// re-uses a single string to pass keys (so using standard string assignment
// will force a re-allocation b/c of shared ref-counting)
//
// this isn't done for values, because all values are read-only in a
// multi-version
// system. ermia::varstrs for values only point to the real data in the
// database, but still we need to allocate a ermia::varstr header for each
// value. Internally it's just a ermia::varstr in the stack.
template <size_t N>
class static_limit_callback : public ermia::OrderedIndex::ScanCallback {
 public:
  // XXX: push ignore_key into lower layer
  static_limit_callback(ermia::str_arena *arena, bool ignore_key)
      : n(0), arena(arena), ignore_key(ignore_key) {
    static_assert(N > 0, "xx");
    values.reserve(N);
  }

  virtual bool Invoke(const char *keyp, size_t keylen,
                      const ermia::varstr &value) {
    ASSERT(n < N);
    ermia::varstr *pv = arena->next(0);  // header only
    pv->p = value.p;
    pv->l = value.l;
    if (ignore_key) {
      values.emplace_back(nullptr, pv);
    } else {
      ermia::varstr *const s_px = arena->next(keylen);
      ASSERT(s_px);
      s_px->copy_from(keyp, keylen);
      values.emplace_back(s_px, pv);
    }
    return ++n < N;
  }

  inline size_t size() const { return values.size(); }

  typedef std::pair<const ermia::varstr *, const ermia::varstr *> kv_pair;
  typename std::vector<kv_pair> values;

 private:
  size_t n;
  ermia::str_arena *arena;
  bool ignore_key;
};

class credit_check_order_line_scan_callback
    : public ermia::OrderedIndex::ScanCallback {
 public:
  credit_check_order_line_scan_callback(uint64_t v) : sum(0), schema_v(v) {}
  inline virtual bool Invoke(const char *keyp, size_t keylen,
                             const ermia::varstr &value) {
    MARK_REFERENCED(keyp);
    MARK_REFERENCED(keylen);
    if (schema_v == 0) {
      order_line::value v_ol_temp;
      const order_line::value *val = Decode(value, v_ol_temp);
      sum += val->ol_amount;
    } else {
      if (ddl_example == 0) {
        order_line_1::value v_ol_temp;
        const order_line_1::value *val = Decode(value, v_ol_temp);
        sum += val->ol_amount;
      } else if (ddl_example == 4) {
        order_line_stock::value v_ol_temp;
        const order_line_stock::value *val = Decode(value, v_ol_temp);
        sum += val->ol_amount;
      }
    }
    return true;
  }
  double sum;
  uint64_t schema_v;
};

class credit_check_order_scan_callback
    : public ermia::OrderedIndex::ScanCallback {
 public:
  credit_check_order_scan_callback(ermia::str_arena *arena) : _arena(arena) {}
  inline virtual bool Invoke(const char *keyp, size_t keylen,
                             const ermia::varstr &value) {
    MARK_REFERENCED(value);
    ermia::varstr *const k = _arena->next(keylen);
    ASSERT(k);
    k->copy_from(keyp, keylen);
    output.emplace_back(k);
    return true;
  }
  std::vector<ermia::varstr *> output;
  ermia::str_arena *_arena;
};

class order_line_nop_callback : public ermia::OrderedIndex::ScanCallback {
 public:
  order_line_nop_callback(uint64_t v) : n(0), schema_v(v) {}
  virtual bool Invoke(const char *keyp, size_t keylen,
                      const ermia::varstr &value) {
    MARK_REFERENCED(keylen);
    MARK_REFERENCED(keyp);
    if (schema_v == 0) {
      ASSERT(keylen == sizeof(order_line::key));
      order_line::value v_ol_temp;
      const order_line::value *v_ol = Decode(value, v_ol_temp);
#ifndef NDEBUG
      order_line::key k_ol_temp;
      const order_line::key *k_ol = Decode(keyp, k_ol_temp);
      checker::SanityCheckOrderLine(k_ol, v_ol);
#endif
    } else {
      ASSERT(keylen == sizeof(order_line_1::key));
      if (ddl_example == 0) {
        order_line_1::value v_ol_temp;
        const order_line_1::value *v_ol = Decode(value, v_ol_temp);
      } else if (ddl_example == 4) {
        order_line_stock::value v_ol_temp;
        const order_line_stock::value *v_ol = Decode(value, v_ol_temp);
      }
    }
    ++n;
    return true;
  }
  size_t n;
  uint64_t schema_v;
};

class latest_key_callback : public ermia::OrderedIndex::ScanCallback {
 public:
  latest_key_callback(ermia::varstr &k, int32_t limit = -1)
      : limit(limit), n(0), k(&k) {
    ALWAYS_ASSERT(limit == -1 || limit > 0);
  }

  virtual bool Invoke(const char *keyp, size_t keylen,
                      const ermia::varstr &value) {
    MARK_REFERENCED(value);
    ASSERT(limit == -1 || n < limit);
    k->copy_from(keyp, keylen);
    ++n;
    return (limit == -1) || (n < limit);
  }

  inline size_t size() const { return n; }
  inline ermia::varstr &kstr() { return *k; }

 private:
  int32_t limit;
  int32_t n;
  ermia::varstr *k;
};

class order_line_scan_callback : public ermia::OrderedIndex::ScanCallback {
 public:
  order_line_scan_callback(uint64_t v, const uint threshold) : n(0), schema_v(v), threshold(threshold) {}
  virtual bool Invoke(const char *keyp, size_t keylen,
                      const ermia::varstr &value) {
    MARK_REFERENCED(keyp);
    MARK_REFERENCED(keylen);
    if (schema_v == 0) {
      ASSERT(keylen == sizeof(order_line::key));
      order_line::value v_ol_temp;
      const order_line::value *v_ol = Decode(value, v_ol_temp);
#ifndef NDEBUG
      order_line::key k_ol_temp;
      const order_line::key *k_ol = Decode(keyp, k_ol_temp);
      checker::SanityCheckOrderLine(k_ol, v_ol);
#endif
      s_i_ids[v_ol->ol_i_id] = 1;
    } else {
      ASSERT(keylen == sizeof(order_line_1::key));
      if (ddl_example == 0) {
        order_line_1::value v_ol_temp;
        const order_line_1::value *v_ol = Decode(value, v_ol_temp);
        s_i_ids[v_ol->ol_i_id] = 1;
      } else if (ddl_example == 4) {
        order_line_stock::value v_ol_s_temp;
        const order_line_stock::value *v_ol_s = Decode(value, v_ol_s_temp);
        // If one s_quantity fits, then all records in order_line_stock
        // with the same ol_i_id fit.
        if (v_ol_s->s_quantity < int(threshold)) {
          s_i_ids[v_ol_s->ol_i_id] = 1;
        }
      }
    }
    n++;
    return true;
  }
  size_t n;
  uint64_t schema_v;
  const uint threshold;
  std::unordered_map<uint, bool> s_i_ids;
};

class new_order_scan_callback : public ermia::OrderedIndex::ScanCallback {
 public:
  new_order_scan_callback() : k_no(0) {}
  virtual bool Invoke(const char *keyp, size_t keylen,
                      const ermia::varstr &value) {
    MARK_REFERENCED(keylen);
    MARK_REFERENCED(value);
    ASSERT(keylen == sizeof(new_order::key));
    ASSERT(value.size() == sizeof(new_order::value));
    k_no = Decode(keyp, k_no_temp);
#ifndef NDEBUG
    new_order::value v_no_temp;
    const new_order::value *v_no = Decode(value, v_no_temp);
    checker::SanityCheckNewOrder(k_no);
#endif
    return false;
  }
  inline const new_order::key *get_key() const { return k_no; }

 private:
  new_order::key k_no_temp;
  const new_order::key *k_no;
};

class tpcc_worker : public bench_worker, public tpcc_worker_mixin {
 public:
  tpcc_worker(unsigned int worker_id, unsigned long seed, ermia::Engine *db,
              const std::map<std::string, ermia::OrderedIndex *> &open_tables,
              const std::map<std::string, std::vector<ermia::OrderedIndex *>>
                  &partitions,
              spin_barrier *barrier_a, spin_barrier *barrier_b,
              uint home_warehouse_id)
      : bench_worker(worker_id, true, seed, db, open_tables, barrier_a,
                     barrier_b),
        tpcc_worker_mixin(partitions),
        home_warehouse_id(home_warehouse_id),
        schema_index(
            (ermia::ConcurrentMasstreeIndex *)open_tables.at("SCHEMA")),
        schema_fid(
            open_tables.at("SCHEMA")->GetTableDescriptor()->GetTupleFid()),
        order_line_fid(open_tables.at("order_line_0")
                           ->GetTableDescriptor()
                           ->GetTupleFid()),
        oorder_fid(
            open_tables.at("oorder_0")->GetTableDescriptor()->GetTupleFid()),
        customer_fid(
            open_tables.at("customer_0")->GetTableDescriptor()->GetTupleFid()),
        order_line_table_index(
            (ermia::ConcurrentMasstreeIndex *)open_tables.at("order_line_0")),
        oorder_table_index(
            (ermia::ConcurrentMasstreeIndex *)open_tables.at("oorder_0")),
        oorder_table_secondary_index((ermia::ConcurrentMasstreeIndex *)
                                         open_tables.at("oorder_c_id_idx_0")),
        customer_table_index(
            (ermia::ConcurrentMasstreeIndex *)open_tables.at("customer_0")),
        customer_table_secondary_index(
            (ermia::ConcurrentMasstreeIndex *)open_tables.at(
                "customer_name_idx_0"))
  {
    ASSERT(home_warehouse_id >= 1 and home_warehouse_id <= NumWarehouses() + 1);
    memset(&last_no_o_ids[0], 0, sizeof(last_no_o_ids));

    const schema_kv::key k1(order_line_fid);
    order_line_key = (ermia::varstr *)ermia::MM::allocate(
        sizeof(ermia::varstr) + sizeof(k1));
    new (order_line_key) ermia::varstr(
        (char *)order_line_key + sizeof(ermia::varstr), sizeof(k1));
    Encode(*order_line_key, k1);

    const schema_kv::key k2(oorder_fid);
    oorder_key = (ermia::varstr *)ermia::MM::allocate(sizeof(ermia::varstr) +
                                                      sizeof(k2));
    new (oorder_key)
        ermia::varstr((char *)oorder_key + sizeof(ermia::varstr), sizeof(k2));
    Encode(*oorder_key, k2);

    const schema_kv::key k3(customer_fid);
    customer_key = (ermia::varstr *)ermia::MM::allocate(sizeof(ermia::varstr) +
                                                        sizeof(k3));
    new (customer_key)
        ermia::varstr((char *)customer_key + sizeof(ermia::varstr), sizeof(k3));
    Encode(*customer_key, k3);
  }

  // XXX(stephentu): tune this
  static const size_t NMaxCustomerIdxScanElems = 512;

  rc_t txn_new_order();

  static rc_t TxnNewOrder(bench_worker *w) {
    return static_cast<tpcc_worker *>(w)->txn_new_order();
  }

  rc_t txn_delivery();

  static rc_t TxnDelivery(bench_worker *w) {
    return static_cast<tpcc_worker *>(w)->txn_delivery();
  }

  rc_t txn_credit_check();
  static rc_t TxnCreditCheck(bench_worker *w) {
    return static_cast<tpcc_worker *>(w)->txn_credit_check();
  }

  rc_t txn_payment();

  static rc_t TxnPayment(bench_worker *w) {
    return static_cast<tpcc_worker *>(w)->txn_payment();
  }

  rc_t txn_order_status();

  static rc_t TxnOrderStatus(bench_worker *w) {
    return static_cast<tpcc_worker *>(w)->txn_order_status();
  }

  rc_t txn_stock_level();

  static rc_t TxnStockLevel(bench_worker *w) {
    return static_cast<tpcc_worker *>(w)->txn_stock_level();
  }

  rc_t txn_microbench_random();

  static rc_t TxnMicroBenchRandom(bench_worker *w) {
    return static_cast<tpcc_worker *>(w)->txn_microbench_random();
  }

  rc_t txn_query2();

  static rc_t TxnQuery2(bench_worker *w) {
    return static_cast<tpcc_worker *>(w)->txn_query2();
  }

  rc_t txn_ddl(uint32_t ddl_example);

  static rc_t TxnDDL(bench_worker *w, uint32_t ddl_example) {
    return static_cast<tpcc_worker *>(w)->txn_ddl(ddl_example);
  }

#ifdef DDL
  inline uint64_t tpcc_read_schema(ermia::transaction *txn, ermia::ConcurrentMasstreeIndex *table_index,
                                   ermia::varstr *table_key, ermia::schema_record &out_schema) {
    ermia::varstr v1;
    ermia::OID oid = ermia::INVALID_OID;
    ermia::catalog::read_schema(txn, schema_index, table_index, *table_key, v1, &oid);

    schema_kv::value schema_value_temp;
    const schema_kv::value *schema_value = Decode(v1, schema_value_temp);
    out_schema.value_to_record(schema_value);
    return out_schema.v;
  }
#endif

  // Specific DDL operations
  rc_t add_column(ermia::transaction *txn, ermia::ddl::ddl_executor *ddl_exe,
                  uint32_t ddl_example);
  rc_t table_split(ermia::transaction *txn, ermia::ddl::ddl_executor *ddl_exe,
                   uint32_t ddl_example);
  rc_t preaggregation(ermia::transaction *txn, ermia::ddl::ddl_executor *ddl_exe,
                      uint32_t ddl_example);
  rc_t create_index(ermia::transaction *txn, ermia::ddl::ddl_executor *ddl_exe,
                    uint32_t ddl_example);
  rc_t table_join(ermia::transaction *txn, ermia::ddl::ddl_executor *ddl_exe,
                  uint32_t ddl_example);
  rc_t add_constraint(ermia::transaction *txn, ermia::ddl::ddl_executor *ddl_exe,
                      uint32_t ddl_example);
  rc_t add_column_and_constraint(ermia::transaction *txn,
                                 ermia::ddl::ddl_executor *ddl_exe,
                                 uint32_t ddl_example);

  virtual workload_desc_vec get_workload() const override;
  virtual ddl_workload_desc_vec get_ddl_workload() const override;

 protected:
  ALWAYS_INLINE ermia::varstr &str(uint64_t size) { return *arena->next(size); }

 private:
  const uint home_warehouse_id;
  int32_t last_no_o_ids[10];  // XXX(stephentu): hack
  ermia::ConcurrentMasstreeIndex *schema_index;
  ermia::FID schema_fid;
  ermia::FID order_line_fid;
  ermia::FID oorder_fid;
  ermia::FID customer_fid;
  ermia::varstr *order_line_key;
  ermia::varstr *oorder_key;
  ermia::varstr *customer_key;
  ermia::ConcurrentMasstreeIndex *order_line_table_index;
  ermia::ConcurrentMasstreeIndex *oorder_table_index;
  ermia::ConcurrentMasstreeIndex *oorder_table_secondary_index;
  ermia::ConcurrentMasstreeIndex *customer_table_index;
  ermia::ConcurrentMasstreeIndex *customer_table_secondary_index;
};

/*class tpcc_cs_worker : public bench_worker, public tpcc_worker_mixin {
 public:
  tpcc_cs_worker(
      unsigned int worker_id, unsigned long seed, ermia::Engine *db,
      const std::map<std::string, ermia::OrderedIndex *> &open_tables,
      const std::map<std::string, std::vector<ermia::OrderedIndex *>>
          &partitions,
      spin_barrier *barrier_a, spin_barrier *barrier_b, uint home_warehouse_id);
  // XXX(stephentu): tune this
  static const size_t NMaxCustomerIdxScanElems = 512;

  ermia::coro::generator<rc_t> txn_new_order(uint32_t idx,
                                             ermia::epoch_num begin_epoch);

  static ermia::coro::generator<rc_t> TxnNewOrder(
      bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<tpcc_cs_worker *>(w)->txn_new_order(idx, begin_epoch);
  }

  ermia::coro::generator<rc_t> txn_delivery(uint32_t idx,
                                            ermia::epoch_num begin_epoch);

  static ermia::coro::generator<rc_t> TxnDelivery(
      bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<tpcc_cs_worker *>(w)->txn_delivery(idx, begin_epoch);
  }

  ermia::coro::generator<rc_t> txn_credit_check(uint32_t idx,
                                                ermia::epoch_num begin_epoch);

  static ermia::coro::generator<rc_t> TxnCreditCheck(
      bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<tpcc_cs_worker *>(w)->txn_credit_check(idx, begin_epoch);
  }

  ermia::coro::generator<rc_t> txn_payment(uint32_t idx,
                                           ermia::epoch_num begin_epoch);

  static ermia::coro::generator<rc_t> TxnPayment(bench_worker *w, uint32_t idx,
                                                 ermia::epoch_num begin_epoch) {
    return static_cast<tpcc_cs_worker *>(w)->txn_payment(idx, begin_epoch);
  }

  ermia::coro::generator<rc_t> txn_order_status(uint32_t idx,
                                                ermia::epoch_num begin_epoch);

  static ermia::coro::generator<rc_t> TxnOrderStatus(
      bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<tpcc_cs_worker *>(w)->txn_order_status(idx, begin_epoch);
  }

  ermia::coro::generator<rc_t> txn_stock_level(uint32_t idx,
                                               ermia::epoch_num begin_epoch);

  static ermia::coro::generator<rc_t> TxnStockLevel(
      bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<tpcc_cs_worker *>(w)->txn_stock_level(idx, begin_epoch);
  }

  ermia::coro::generator<rc_t> txn_query2(uint32_t idx,
                                          ermia::epoch_num begin_epoch);

  static ermia::coro::generator<rc_t> TxnQuery2(bench_worker *w, uint32_t idx,
                                                ermia::epoch_num begin_epoch) {
    return static_cast<tpcc_cs_worker *>(w)->txn_query2(idx, begin_epoch);
  }

  ermia::coro::generator<rc_t> txn_microbench_random(
      uint32_t idx, ermia::epoch_num begin_epoch);

  static ermia::coro::generator<rc_t> TxnMicroBenchRandom(
      bench_worker *w, uint32_t idx, ermia::epoch_num begin_epoch) {
    return static_cast<tpcc_cs_worker *>(w)->txn_microbench_random(idx,
                                                                   begin_epoch);
  }

  virtual workload_desc_vec get_workload() const override;
  virtual ddl_workload_desc_vec get_ddl_workload() const override;
  virtual void MyWork(char *) override;

 protected:
  ALWAYS_INLINE ermia::varstr &str(ermia::str_arena &a, uint64_t size) {
    return *a.next(size);
  }

 private:
  const uint home_warehouse_id;
  int32_t last_no_o_ids[10];  // XXX(stephentu): hack
};*/

#endif  // ADV_COROUTINE
