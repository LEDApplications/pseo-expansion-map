# PSEO Partners Map

Generate the data for the [PSEO Expansion Map](https://observablehq.com/@jodyhoonstarr/pseo-expansion-2024-update)

> Disclaimer: The data generated here should not be used for analysis. It's cobbling together a few things to tell a story about the growth of the PSEO partnership.

## Install deps

This project uses [conda](https://conda.io/projects/conda/en/latest/user-guide/install/index.html).

```sh
# install deps
conda env create --file=environment.yaml

# activate
conda activate pseo-partners-map
```

## Generate Data

```sh
# create the output file
./generate.py -r -o viz_data.csv

# create the output file and the intermediate sqlite database
# add the -f/--force flag to replace an existing intermediate database
./generate.py -r -d ./pseo.db -o viz_data.csv
```

## Source Data

### Grad Counts

This process uses all public PSEO earnings vintages ([link](https://lehd.ces.census.gov/data/pseo/)) to identify when an institution joined the partnership. The grad totals per institution are from the IPEDS counts attached to the earnings files.

> The initial release was not used as it's in a different format and doesn't contain gradcounts

### Crosswalk

The pseo data uses the opeid while the school locations use a unit id. Some crosswalks are available [here](https://collegescorecard.ed.gov/data/) which are stored in the `data/` directory. No effort has gone into syncing data vintages or doing cleanup on 3rd party sources.

> 61 institutions couldn't be crosswalked from OPEID <-> UNITID

### School Locations

The institution names and locations are taken from [NCES](https://nces.ed.gov/programs/edge/geographic/schoollocations) and saved as a csv to the `data/` directory in this repo. These locations use a `UNITID` which requires the crosswalk above.

> 2 institutions were given an unitid that has no entry in the location data
