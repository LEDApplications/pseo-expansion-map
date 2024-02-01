# PSEO Partners Map

Generate the data for the [Partners Map](https://observablehq.com/@jodyhoonstarr/pseo-expansion)

## Generate Data

```sh
# create the output file
./generate.py -r

# create the output file and the intermediate sqlite database
./generate.py -r -d ./pseo.db
```

## School Locations

The institution names and locations are taken from [NCES](https://nces.ed.gov/programs/edge/geographic/schoollocations) and saved as a csv to the `data/` directory in this repo. 