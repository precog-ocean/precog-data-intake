import sys
import os

from ipywidgets import Password

sys.path.append(os.path.dirname(os.path.abspath(__name__)))
from scripts.intake_UtilFuncs import print_precog_header, print_precog_footer
#sys.path.append('../') #This shorthand makes one module visible to the other, When modules are in parallel locations
import intake_esgf
from intake_esgf_mods.catalog import ESGFCatalog
from scripts.intake_UtilFuncs import *

def test_print_decorators():
    assert print_precog_footer() == None
    assert print_precog_header() == None

def test_search_to_dict(var='o2'):
    variable_ids = [var]
    for oceanvar in variable_ids:
        cat = ESGFCatalog().search(project='CMIP6',
                                   activity_drs=['CMIP'],
                                   experiment_id=['piControl', 'historical'],
                                   frequency='mon',
                                   variable_id=oceanvar,
                                   grid_label=['gn', 'gr'])

        cat = cat.remove_ensembles()  # filters out to keep only a single member from the ensemble (i.e., the one with the lowest variant label (see below)
        # Complete TODO add function to esgf-intake catalog.py file, so that download is not automatic, ...
        # rather we'd like to retrieve the attributes from the search across all nodes into a DataFrame ...
        # so we can see what is available and where, and do further filtering without having to download heaps of data unnecessarily.
        info = cat.infos_to_dict(
            quiet=False)  # dictionary structure containing all the urls, SHA256 hashes to enable serialised download
        DataFrameSearch = pd.DataFrame.from_dict(
            info['https'])  # all with monthly piControl and historical salinity and potential temperature

    assert bool(info) == True
    assert len(DataFrameSearch) != 0
    assert append_cols(DataFrameSearch)['variable_id'].unique() == [var]

def test_append_cols(var='thetao'):
    variable_ids = [var]
    for oceanvar in variable_ids:
        cat = ESGFCatalog().search(project='CMIP6',
                                   activity_drs=['CMIP'],
                                   experiment_id=['piControl', 'historical'],
                                   frequency='mon',
                                   variable_id=oceanvar,
                                   grid_label=['gn', 'gr'])

        cat = cat.remove_ensembles()  # filters out to keep only a single member from the ensemble (i.e., the one with the lowest variant label (see below)
        # Complete TODO add function to esgf-intake catalog.py file, so that download is not automatic, ...
        # rather we'd like to retrieve the attributes from the search across all nodes into a DataFrame ...
        # so we can see what is available and where, and do further filtering without having to download heaps of data unnecessarily.
        info = cat.infos_to_dict(
            quiet=False)  # dictionary structure containing all the urls, SHA256 hashes to enable serialised download
        DataFrameSearch = pd.DataFrame.from_dict(
            info['https'])  # all with monthly piControl and historical salinity and potential temperature

        DF = append_cols(DataFrameSearch)
        print(DF.columns)
    assert bool(info) == True
    assert len(DataFrameSearch) != 0
    assert append_cols(DataFrameSearch)['variable_id'].unique() == [var]
    assert set(['source_id', 'experiment_id', 'variant_label', 'frequency', 'variable_id', 'grid_label']).issubset(DF.columns)

def test_patch_date():

    df = pd.DataFrame(
        {
            "path": [Path('CMIP6/CMIP/HAMMOZ-Consortium/MPI-ESM-1-2-HAM/piControl/r1i1p1f1/Omon/expc/gn/v20200120/expc_Omon_MPI-ESM-1-2-HAM_piControl_r1i1p1f1_gn_185001-186912.nc'),
                     Path('CMIP6/CMIP/HAMMOZ-Consortium/MPI-ESM-1-2-HAM/piControl/r1i1p1f1/Omon/expc/gn/v20200120/expc_Omon_MPI-ESM-1-2-HAM_piControl_r1i1p1f1_gn_187001-188912.nc')],
            "file_start": [None, None],
            "file_end": [None, None],
        }
    )

    patched = patch_date(df)

    assert patched.loc[0, "file_start"] == pd.Timestamp(datetime.date(1850, 1, 1))
    assert patched.loc[0, "file_end"] == pd.Timestamp(datetime.date(1869, 12, 1))
    assert patched.loc[1, "file_start"] == pd.Timestamp(datetime.date(1870, 1, 1))
    assert patched.loc[1, "file_end"] == pd.Timestamp(datetime.date(1889, 12, 1))

#resume
def test_check_var_in():
    pass

def test_check_continuity():
    pass

def test_catalogue_traverser():
    pass

def test_check_grid_avail():
    pass

def test_instantiate_logging_file():
    pass

def test_check_url_validity():
    pass

def test_link_traverser():
    pass

def test_save_searched_tests():
    pass

def test_import_ocean_std_names():
    pass


## NOW functions inside main ocean var DL script

def test_verify_hash():
    pass

def test_calculate_hash():
    pass

def test_download_file():
    pass

def test_download_speed():
    pass

## NOW functions inside main cell measure DL script

def test_import_ocean_var_names():
    pass

def test_varcell_prepare_df():
    pass

