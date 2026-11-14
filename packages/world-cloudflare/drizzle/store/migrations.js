import journal from './meta/_journal.json';
import m0000 from './0000_baseline.sql';
import m0001 from './0001_sharding.sql';
import m0002 from './0002_token_shards.sql';
import m0003 from './0003_spec9_force_claims.sql';
import m0004 from './0004_start_hooks.sql';
import m0005 from './0005_list_index.sql';

  export default {
    journal,
    migrations: {
      m0000,
m0001,
m0002,
m0003,
m0004,
m0005
    }
  }
  