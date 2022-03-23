#include "catalog_mgr.h"

namespace ermia {

void create_schema_table(ermia::Engine *db, const char *name) {
  ermia::thread::Thread *thread = ermia::thread::GetThread(true);
  ALWAYS_ASSERT(thread);

  auto create_table = [=](char *) {
    db->CreateTable(name);
    db->CreateMasstreePrimaryIndex(name, std::string(name));
    ermia::schema_td = ermia::Catalog::GetTable(name);
  };

  thread->StartTask(create_table);
  thread->Join();
  ermia::thread::PutThread(thread);
}

}  // namespace ermia
