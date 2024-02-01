# PSEO Partners Map

Generate the data for the [PSEO Partners Map](https://observablehq.com/@jodyhoonstarr/pseo-expansion-2024-update)

## Generate Data

```sh
# create the output file
./generate.py -r

# create the output file and the intermediate sqlite database
./generate.py -r -d ./pseo.db
```

## Crosswalk

The pseo data uses the opeid while the school locations use a unit id. Some crosswalks are available [here](https://collegescorecard.ed.gov/data/) which are stored in the `data/` directory. No effort has gone into syncing data vintages or doing cleanup on 3rd party sources.

> 61 institutions couldn't be crosswalked from OPEID <-> UNITID

## School Locations

The institution names and locations are taken from [NCES](https://nces.ed.gov/programs/edge/geographic/schoollocations) and saved as a csv to the `data/` directory in this repo. These locations use a `UNITID` which requires the crosswalk above.

> 2 institutions were given an unitid that has no entry in the location data
