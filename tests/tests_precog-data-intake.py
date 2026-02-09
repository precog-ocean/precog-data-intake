import sys
import os

from ipywidgets import Password, Datetime
from pandas.core.interchange.dataframe_protocol import DataFrame

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

def test_check_var_in():

    vars = ['epc100', 'o2']

    ConcatDF = pd.DataFrame()
    for oceanvar in vars:
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
        ConcatDF = pd.concat([ConcatDF, DF])

    vars.append('thetao')
    catch_state = []
    for var in vars : # expected behavious is catch= (True for 'epc100'), (True for 'o2') and (False for 'thetao')
        df1_subset_var = ConcatDF.loc[ConcatDF['variable_id'] == var]  # subsetting for a single variable
        if len(df1_subset_var) == 0:  # in case any catalogue entries do not have results for the variable, then move on to the next model
            # print(f"Model {model} returned {len(df1_subset_var)} entries for variable {var}")
            catch_state.append(False)
        else:
            catch_state.append(True)

    assert catch_state == [True, True, False]

def test_check_continuity():

    test_DF = pd.read_excel(os.path.join('tests', 'test_DF_Search.xlsx'))
    test_DF.loc[:, 'file_start'] = None #changed on purpose to trigger patch date

    models = test_DF['source_id'].unique().tolist()
    vars = test_DF['variable_id'].unique().tolist()
    test_DF = test_DF[test_DF['variable_id'] =='o2']# single var DF
    run = 'piControl'
    o2_start = "5201-01-01"

    for model in [models[0]]:
        DataFrameSubset = test_DF[test_DF['source_id'] == model]
        DataFrameSubset = DataFrameSubset.loc[DataFrameSubset['experiment_id'] == run].reset_index(drop=True)

        if None in DataFrameSubset['file_start'].unique() or None in DataFrameSubset['file_end'].unique():
            print('Date patching needed. Function to correct date formats triggered.')
            DataFrameSubset = patch_date(DataFrameSubset)

        date_strs_start = DataFrameSubset['file_start'].reset_index(drop=True)
        date_strs_end = DataFrameSubset['file_end'].reset_index(drop=True)

        date_strs_start.values[0] = o2_start

        # check if both columns have a datatype as 'Timestamp' so that operations can be done on dates, if not convert to Timestamp
        if type(date_strs_start.values[0]) == str:
            print("converting string to timestamp")
            date_strs_start.values[0] = pd.Timestamp(date_strs_end.values[0])
            assert(DataFrameSubset.loc[0,'file_start'] == pd.Timestamp(o2_start)) # check if correctly patched

        gaps_within = [j.date().year - i.date().year for i, j in
                       zip(date_strs_start, date_strs_end)]  # #the year gap within each nc file

        if len(DataFrameSubset) != 1:  # if there's more than one nc file
            single_file = False
            gaps_across = [j.date().year - i.date().year for i, j in
                           zip(date_strs_start[:-1],
                               date_strs_start[1:])]  # the year time step between consecutive nc files

            # padding with an extra dummy line for loop to work
            date_strs_start_pad = pd.concat([date_strs_start, pd.Series(date_strs_start.iloc[-1])]).reset_index(
                drop=True)
            date_strs_end_pad = pd.concat([date_strs_end, pd.Series(date_strs_end.iloc[-1])]).reset_index(drop=True)

            gaps_within.append(gaps_within[-1])
            gaps_across.append(gaps_across[-1])  # repeat line as across always has size n-1
            gaps_across.append(gaps_across[-1])

            range_padded = list(range(0, len(date_strs_start) + 1))

            traverser = list(zip(date_strs_start_pad, date_strs_end_pad, gaps_within, gaps_across, range_padded))

            consecutive_test = []
            counter = 0

            for i in range(0, len(traverser) - 1):
                a, b, gap_lat_i, gap_vert_i, indx_i = traverser[i]
                c, d, gap_lat_ii, gap_vert_ii, indx_ii = traverser[i + 1]
                if (a.date().year + gap_lat_i == b.date().year
                        and b.date().year + 1 == c.date().year
                        and c.date().year + gap_lat_ii == d.date().year):
                    counter += 1
                    consecutive_test.append(True)

                else:
                    consecutive_test.append(False)

                if i + 1 == len(traverser) - 1:  # prints last line once reaches the end
                    print(f'{traverser[i][0]} and {traverser[i + 1][1]}')

            assert(single_file == False)
            assert(all(consecutive_test[:-1]))

def test_catalogue_traverser():
    pass

def test_check_grid_avail():

    DataFrameSubsetModel = pd.read_excel(os.path.join('tests', 'test_DF_Search.xlsx'))
    model = list(DataFrameSubsetModel['source_id'].unique())[1] # 'CMCC-ESM2'
    DataFrameSubsetModel = DataFrameSubsetModel.loc[DataFrameSubsetModel['source_id'] == model]
    varlist = ['so', 'o2', 'thetao']

    dict_true = {'grid_label': ['gn', 'gr', 'gn', 'gr'],
                 'var_test': [[True, True, True],
                              [False, False, False],
                              [True, True, True],
                              [False, False, False]],
                 'variable_ids': [['o2', 'so', 'thetao'],
                                  ['o2', 'so', 'thetao'],
                                  ['o2', 'so', 'thetao'],
                                  ['o2', 'so', 'thetao']],
                 'has_all_variables': [True, False, True, False],
                 'run': ['piControl', 'piControl', 'historical', 'historical'],
                 'model': ['CMCC-ESM2', 'CMCC-ESM2', 'CMCC-ESM2', 'CMCC-ESM2']}

    # tests if all variables being queried have matching grids
    # example: Even though NorESM2-LM has thetao and so for the native grid, it does not have o2, then flag it and do not intake native grid files.
    # ...NorESM2-LM has all thetao, so, and o2 in the regular grid tho, so this should be included and processed further


    catch_state = True

    dict_test = {'grid_label': [],
            'var_test': [],
            'variable_ids': [],
            'has_all_variables': [],
            'run': [],
            'model': []}

    for run in ["piControl", "historical"] :
        DataFrameSubsetModel_run = DataFrameSubsetModel.loc[DataFrameSubsetModel['experiment_id'] == run]
        for grid in ["gn", "gr"]:
            DataFrameSubsetModel_grid = DataFrameSubsetModel_run.loc[DataFrameSubsetModel_run['grid_label'] == grid]  # subsetting for a grid type
            var_test = list(DataFrameSubsetModel_grid['variable_id'].unique())

            vart_test_ordered = set(var_test)
            varlist_ordered = set(varlist)

            var_test_final = [i==j for i, j in zip(varlist_ordered, vart_test_ordered)]

            if varlist_ordered == vart_test_ordered:
                catch_state = True

            else:
                missing_var = set(varlist_ordered)- set(vart_test_ordered)
                # padding for comparison between lists
                # make a list first
                vart_test_ordered = list(vart_test_ordered)
                varlist_ordered = list(varlist_ordered)

                string_flag = '!'
                for missing in missing_var:
                    idx = varlist_ordered.index(missing)
                    vart_test_ordered.insert(idx, missing + string_flag)

                # find indices of missing items and append them to specific positions followed by "!"
                catch_state = False
                var_test_final = [i == j for i, j in zip(varlist_ordered, vart_test_ordered)]

            dict_test['var_test'].append(var_test_final)
            dict_test['run'].append(run)
            dict_test['has_all_variables'].append(catch_state)
            dict_test['model'].append(model)
            dict_test['variable_ids'].append(varlist_ordered)
            dict_test['grid_label'].append(grid)

    assert (dict_test == dict_true)


#resume from here
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

