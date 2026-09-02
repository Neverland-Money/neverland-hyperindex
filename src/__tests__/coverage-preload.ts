// c8's `--all` loader and tsx can emit different source maps for production
// files imported only by one Node test worker. Preload the dependency-neutral
// growth modules in every coverage worker so their real maps merge correctly.
import '../helpers/lpGrowthMath';
import '../handlers/lpGrowth';
