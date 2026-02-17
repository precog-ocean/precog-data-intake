# Running tests

To run the tests, simply execute `tests/tests_precog-data-intake.py` by running on the terminal:


```bash
 > source .venv/bin/activate #activate the pyhton enviroment 
 > pytest tests/tests_precog-data-intake.py -s -vv #run test suite with verbose and printouts enabled
```

The template dataframe `test_DF_Seach.xlsx` is ingested by the tests.

Test files simulating corrupt downloads, are generated under `./tests/tests_dl`.

Search results and discovery of `areacello` grid cell measures are also simulated for a single model `CanESM5`.

Discovery results are also saved under a directory tree structure for inspection. 