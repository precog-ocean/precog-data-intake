import os
import pandas as pd
import time
import logging
import requests
from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from ascii_magic import AsciiArt
import datetime
import sys

sys.path.append(os.path.dirname(os.path.abspath(__name__)))


def print_precog_header():
    print("\n" * 2)
    my_art = AsciiArt.from_image('./misc_images/precog_logo_full.png')  # path to logo image
    my_art.to_terminal(width_ratio=3)
    print("\n" * 2)
    return None


def print_precog_footer():
    end_art = AsciiArt.from_image('./misc_images/squid2.png')
    end_art.to_terminal(columns=120, width_ratio=2.5)
    print("\n" * 2)
    return None


def append_cols(PandasDataFrame):
    col_names = ['source_id', 'experiment_id', 'variant_label', 'frequency', 'variable_id', 'grid_label']
    positional_order = [3, 4, 5, 6, 7, 8]

    for col, idx in zip(col_names, positional_order):
        print(f"Extracting info from dataset_id the following facet: '{col}' which appears as argument number: {idx}")
        PandasDataFrame[[col]] = PandasDataFrame["dataset_id"].apply(lambda x: pd.Series(str(x).split(".")[idx]))

    return PandasDataFrame  # this is the dataframe with appended columns that make it easier to sort and cross-check across models and variables


def patch_date(DataFrameSubset):
    # get file name to strip dates from

    DataFrameSubset_patched = DataFrameSubset.copy(deep=True)

    for idx in range(0, len(DataFrameSubset_patched['path'])):
        # intentionally ensure this is a Path data type otherwise it clunks sometimes and tries to convert to string unintentionally.
        fname = DataFrameSubset_patched['path'][idx]
        fname = Path(fname)

        start_dt = fname.name.split('_')[-1].split('.nc')[0].split('-')[0]
        start_dt_1 = datetime.date(year=int(start_dt[:4]), month=int(start_dt[4:]), day=1)
        start_dt_1 = pd.Timestamp(start_dt_1)  # ('YYYY-MM-DD HH:MM:SS')

        end_dt = fname.name.split('_')[-1].split('.nc')[0].split('-')[-1]
        end_dt_1 = datetime.date(year=int(end_dt[:4]), month=int(end_dt[4:]), day=1)
        end_dt_1 = pd.Timestamp(end_dt_1)  # ('YYYY-MM-DD HH:MM:SS')

        if DataFrameSubset_patched.loc[idx, 'file_start'] == None:
            DataFrameSubset_patched.loc[idx, 'file_start'] = start_dt_1

        if DataFrameSubset_patched.loc[idx, 'file_end'] == None:
            DataFrameSubset_patched.loc[idx, 'file_end'] = end_dt_1

    return DataFrameSubset_patched


def check_var_in(ConcatDF, varlist):
    catch_state = []
    for var in varlist:
        df1_subset_var = ConcatDF.loc[ConcatDF['variable_id'] == var]  # subsetting for a single variable

        if len(df1_subset_var) == 0:  # in case any catalogue entries do not have results for the variable, then move on to the next model
            # print(f"Model {model} returned {len(df1_subset_var)} entries for variable {var}")
            catch_state.append(False)
        else:
            catch_state.append(True)
    return catch_state


def check_continuity(DataFrameSubset, run, logger):
    DataFrameSubset = DataFrameSubset.sort_values(by=['file_start']).reset_index(drop=True)
    var = DataFrameSubset['variable_id'].unique().tolist()
    model = DataFrameSubset['source_id'].unique().tolist()

    # some fetched dates are returning 'None' for file_start and file_end columns.
    # So it needs to be built and patched manually. Example is model ['SAM0-UNICON']
    if None in DataFrameSubset['file_start'].unique() or None in DataFrameSubset['file_end'].unique():
        print('Date patching needed. Function to correct date formats triggered.')
        DataFrameSubset = patch_date(DataFrameSubset)

    date_strs_start = DataFrameSubset['file_start'].reset_index(drop=True)
    date_strs_end = DataFrameSubset['file_end'].reset_index(drop=True)

    # Complete TODO fix data column so when exporting to spreadsheet all dates for file_start and file_end have the same date format
    # example of date being saved wrong

    # example of date output
    # #o2_Omon_CanESM5_piControl_r1i1p1f1_gn_520101-521012.nc'
    # # 'file_start': Timestamp('5201-01-01 00:00:00'), 'file_end': Timestamp('5210-12-01 00:00:00)'
    # check if both columns have a datatype as 'Timestamp' so that operations can be done on dates, if not convert to Timestamp
    if type(date_strs_start.values[0]) == str:
        date_strs_start = [pd.Timestamp(value) for value in date_strs_start.values]

    if type(date_strs_end.values[0]) == str:
        date_strs_end = [pd.Timestamp(value) for value in date_strs_end.values]

    # gap_in_file = date_strs_end[0].date().year - date_strs_start[0].date().year #the year time step within each nc file
    # gap_across_file = date_strs_start[1].date().year-date_strs_start[0].date().year #the year time step between consecutive nc files

    gaps_within = [j.date().year - i.date().year for i, j in
                   zip(date_strs_start, date_strs_end)]  # #the year gap within each nc file

    if len(DataFrameSubset) != 1:  # if there's more than one nc file
        single_file = False
        gaps_across = [j.date().year - i.date().year for i, j in
                       zip(date_strs_start[:-1],
                           date_strs_start[1:])]  # the year time step between consecutive nc files
        # padding with an extra dummy line for loop to work
        date_strs_start_pad = pd.concat([date_strs_start, pd.Series(date_strs_start.iloc[-1])]).reset_index(drop=True)
        date_strs_end_pad = pd.concat([date_strs_end, pd.Series(date_strs_end.iloc[-1])]).reset_index(drop=True)

        gaps_within.append(gaps_within[-1])

        gaps_across.append(gaps_across[-1])  # repeat line as across always has size n-1
        gaps_across.append(gaps_across[-1])

        range_padded = list(range(0, len(date_strs_start) + 1))

        traverser = list(zip(date_strs_start_pad, date_strs_end_pad, gaps_within, gaps_across, range_padded))

        consecutive_test = []
        counter = 0
        logger.info(f'Checking if {run} run for variable {var} in {model} is consecutive')
        # print(f'Checking if {run} run for variable {var} in {model} is consecutive')
        for i in range(0, len(traverser) - 1):
            a, b, gap_lat_i, gap_vert_i, indx_i = traverser[i]
            c, d, gap_lat_ii, gap_vert_ii, indx_ii = traverser[i + 1]
            if (a.date().year + gap_lat_i == b.date().year
                    and b.date().year + 1 == c.date().year
                    and c.date().year + gap_lat_ii == d.date().year):
                logger.info(f'{traverser[i][0]} and {traverser[i + 1][0]}')
                counter += 1
                consecutive_test.append(True)

            else:
                consecutive_test.append(False)

            if i + 1 == len(traverser) - 1:  # prints last line once reaches the end
                logger.info(f'{traverser[i][0]} and {traverser[i + 1][1]}')
                # print(f'{traverser[i][0]} and {traverser[i + 1][1]}')

    else:  # in case there's only one nc file
        logger.info(f'Single model output file for run {run}, consider checking. Otherwise it is considered continuous')
        # print(f'Single model output file for run {run}, consider checking. Otherwise, it is considered continuous')
        single_file = True
        counter = 1
        consecutive_test = [True, True]

    return counter, single_file, consecutive_test[:-1]  # removes the last boolean of consecutive_test because this involves the padded test


def catalog_traverser(logger, CatalogDF, varlist):
    if type(varlist) == type(
            'str'):  # if evaluates to a single variable passed as a string (i.e., not a list of strings)
        varlist = [varlist]  # then force it to be a list

    models = CatalogDF['source_id'].unique().tolist()
    models_to_discard = []  # incomplete models (either lack pi or historical)
    model_DF_test_grids_concatenated = pd.DataFrame(
        columns=['grid_label', 'var_test', 'variable_ids', 'has_all_variables', 'run', 'model'])
    df_downloadable = pd.DataFrame(
        columns=['key', 'dataset_id', 'checksum_type', 'checksum', 'size', 'HTTPServer', 'OPENDAP', 'Globus', 'path',
                 'file_start', 'file_end', 'source_id', 'experiment_id', 'variant_label', 'frequency', 'variable_id',
                 'grid_label'])

    for model in models:
        # check if complete/contiguous piControl followed by historical run in both thetao, so and o2 searches
        df1_subset = CatalogDF.loc[CatalogDF['source_id'] == model]

        # TODO make sure each dataframe is continuous with respect to picontrol and historical runs across all variables

        variables_in = check_var_in(df1_subset, varlist)  # checks if any of the variables returns an empty dataframe, if they all return entries then proceed

        if not all(variables_in):
            logger.info(
                f'The model: {model} does not have all variable(s) of interest: test for {varlist} returned {variables_in} \n')
            models_to_discard.append(model)

        elif all(variables_in):  # for the models which have all variables of interest

            # TODO add grid avail check across all variables
            test_grids_pi = check_grid_avail(df1_subset, varlist, grid_labels=['gn', 'gr'], run='piControl',
                                             logger=logger)
            test_grids_historical = check_grid_avail(df1_subset, varlist, grid_labels=['gn', 'gr'], run='historical',
                                                     logger=logger)

            # todo merge both dictionaries and turn into dataframe to export for further inspection and logging
            model_DF_test_grids = pd.concat(
                [pd.DataFrame.from_dict(test_grids_pi), pd.DataFrame.from_dict(test_grids_historical)])
            model_DF_test_grids_concatenated = pd.concat([model_DF_test_grids_concatenated,
                                                          model_DF_test_grids])  # append to master dataframe which is exported at the end with grid and var comparissons

            # check if at least 1 piControl and 1 historical run have all vars in either grid
            # complete todo test two conditions
            lack_pI = model_DF_test_grids.loc[
                (model_DF_test_grids["run"] == 'piControl') & (model_DF_test_grids["has_all_variables"] == True)].empty
            lack_historical = model_DF_test_grids.loc[
                (model_DF_test_grids["run"] == 'historical') & (model_DF_test_grids["has_all_variables"] == True)].empty

            if not lack_pI and not lack_historical:
                logger.info(
                    f'The model: {model} has variables: test for {varlist} returned {variables_in} for both piControl and historical runs.OBS: grid may vary')

                # clean df1_subset so that only gridded files that passed the test are kept in the dataframe
                grid_to_keep = model_DF_test_grids.loc[model_DF_test_grids["has_all_variables"] == True]
                grids_kept = list(set(list(grid_to_keep['grid_label'])))

                filter = df1_subset['grid_label'].isin(grids_kept)
                df1_subset_filtered = df1_subset[filter]

                for var in varlist:

                    df1_subset_var = df1_subset_filtered.loc[df1_subset_filtered['variable_id'] == var]
                    # now checking continuity between piControl and Historical for each variable
                    logger.info(f'Now checking continuity between piControl and Historical for variable {var}')

                    df1_pi = df1_subset_var.loc[
                        df1_subset_var['experiment_id'] == 'piControl']  # subsetting into temporary dataframes
                    df1_historical = df1_subset_var.loc[df1_subset_var['experiment_id'] == 'historical']

                    # checking if both piControl and Historical runs exist for all variables # this is redundant #check if ok to remove for simplicity
                    if len(df1_pi) == 0 or len(df1_historical) == 0:
                        if len(df1_pi) == 0:
                            logger.info(f'No piControl run for model {model} for variable {var}')
                            # TODO remove model from list
                            models_to_discard.append(model)
                            continue

                        if len(df1_historical) == 0:
                            logger.info(f'No Historical run for model {model} for variable {var}')
                            # TODO remove model from list
                            models_to_discard.append(model)
                            continue

                    else:
                        logger.info(f'Found both piControl and Historical runs for model {model} for variable {var}')
                        # check if only one variant and that the variant matches across piControl and Historical

                        # if more than one variant passed, then grab simplest and check if also present in historical. Drop other variants
                        variant_in_common = set(df1_pi['variant_label'].unique()).intersection(
                            df1_historical['variant_label'].unique())
                        # now dropping whatever variant is not captured by intersection
                        df1_pi = df1_pi[df1_pi['variant_label'].isin(list(variant_in_common))]
                        df1_historical = df1_historical[df1_historical['variant_label'].isin(list(variant_in_common))]

                        if variant_in_common:
                            logger.info(
                                f'Variant labels {variant_in_common} from piCrontrol and Historical in model {model} are matching')

                            # check piControl and Historical continuity in dates for each variable
                            # Complete TODO check continuity piControl pass to function
                            counter_piControl, single_file_pi, consecutive_piControl = check_continuity(df1_pi,
                                                                                                        run='piControl',
                                                                                                        logger=logger)  # check piControl continuity
                            if all(consecutive_piControl):
                                logger.info(f'piControl run for variable {var} in {model} looks consecutive')
                                logger.info(f'number of piControl files is {counter_piControl + 1} END\n')

                            # check Historical continuity
                            # complete TODO same scheme as above but for historical run
                            counter_historical, single_file_historical, consecutive_historical = check_continuity(
                                df1_historical, run='historical', logger=logger)
                            if all(consecutive_historical):
                                logger.info(f'Historical run for variable {var} in {model} looks consecutive')
                                logger.info(f'number of Historical files is {counter_historical + 1} END \n')

                            if all(consecutive_piControl) and all(consecutive_historical):
                                df_downloadable = pd.concat([df_downloadable, df1_pi, df1_historical], axis=0)

                            if not all(consecutive_piControl) or not all(consecutive_historical):
                                models_to_discard.append(
                                    model)  # shows which models have both piControl and Historical runs for the following variables: thetao, so, o2, and expc
                                logger.info(
                                    f'Either piControl or Historical runs for model {model} for variable {var} is not continuous')
                                logger.info('\n')

                        else:
                            models_to_discard.append(model)
                            raise ValueError(
                                f'Mismatch between available variant lables \n {df1_pi['variant_label'].unique()} is different from {df1_historical['variant_label'].unique()}')


            else:
                logger.info('\n')
                logger.info(
                    f'No complete set of variables for model {model} for variable {varlist} in either grid. Test returned:')
                string_pretty = ''.join(['#'] * 100)
                logger.info(string_pretty)
                [logger.info(_) for _ in model_DF_test_grids.to_string(index=False).split('\n')]
                # logger.info(model_DF_test_grids.to_string())
                logger.info(string_pretty)
                logger.info('\n')
                models_to_discard.append(model)

    return models_to_discard, model_DF_test_grids_concatenated, df_downloadable


def check_grid_avail(DataFrameSubsetModel, varlist, grid_labels, run, logger):
    # tests if all variables being queried have matching grids
    # example: Even though NorESM2-LM has thetao and so for the native grid, it does not have o2, then flag it and do not intake native grid files.
    # ...NorESM2-LM has all thetao, so, and o2 in the regular grid tho, so this should be included and processed further

    model = list(DataFrameSubsetModel['source_id'].unique())[0]
    DataFrameSubsetModel_run = DataFrameSubsetModel.loc[DataFrameSubsetModel['experiment_id'] == run]
    catch_state = True

    dict = {'grid_label': [],
            'var_test': [],
            'variable_ids': [],
            'has_all_variables': [],
            'run': [],
            'model': []}

    for grid in grid_labels:
        DataFrameSubsetModel_grid = DataFrameSubsetModel_run.loc[
            DataFrameSubsetModel_run['grid_label'] == grid]  # subsetting for a grid type
        # now get list of vars available in that specific grid:
        var_test = list(DataFrameSubsetModel_grid['variable_id'].unique())
        vart_test_mod = set(var_test)
        varlist_mod = set(varlist)

        if vart_test_mod == varlist_mod:  # test if model has a grid output across all variables of interest
            logger.info(
                f'Model {model} has the output(s) for variable(s) {varlist_mod} in a {grid} grid for the {run} run')

            catch_state = True
            var_test_final = [i == j for i, j in zip(varlist_mod, vart_test_mod)]

        else:
            missing_var = varlist_mod - vart_test_mod #gets the set difference
            logger.info(
                f'Model {model} does not have output(s) for variable(s) {varlist} in a {grid} grid for the {run} run')
            logger.info(f'Test returned the following var(s) only {vart_test_mod}')
            logger.info(f'Variable {missing_var} is missing. Ignoring model output for grid {grid}.\n')

            # padding for comparison between lists
            vart_test_mod = sorted(list(vart_test_mod)) # turn it back to a list
            varlist_mod = sorted(list(varlist_mod))

            string_flag = '!' #append symbol next to var to indicate absence
            for missing in missing_var:
                idx = varlist_mod.index(missing)
                vart_test_mod.insert(idx, missing + string_flag)

            # find indices of missing items and append them to specific positions followed by "!"
            catch_state = False
            var_test_final = [i == j for i, j in zip(varlist_mod, vart_test_mod)]

        dict['var_test'].append(var_test_final)
        dict['run'].append(run)
        dict['has_all_variables'].append(catch_state)
        dict['model'].append(model)
        dict['variable_ids'].append(varlist_mod)
        dict['grid_label'].append(grid)

    return dict

def instantiate_logging_file(logfilename, logger_name):
    formatter_line_style = '%(asctime)s - %(levelname)s - %(message)s'
    # Create a logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    # Create a formatter to define the log format
    formatter = logging.Formatter(formatter_line_style)

    # Create a file handler to write logs to a file
    file_handler = logging.FileHandler(logfilename)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Create a stream handler to print logs to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # You can set the desired log level for console output
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


def check_url_validity(iterable):
    urls = iterable[0]
    file_ids = iterable[1]
    links_working = []
    file_downloadable = []

    for url in urls:
        response = requests.get(url, stream=True)
        time.sleep(5)
        if response.status_code == 200:
            links_working.append((True, url, file_ids))
            break
        else:
            links_working.append((False, url, file_ids))
            continue

    if any(links_working):
        file_downloadable.append(True)
    else:
        file_downloadable.append(False)

    return (links_working, file_downloadable)


def link_traverser(DownloadableDF):
    # now traverse through dataframes named df_downloadable testing links for good connection - Complete - TODO change this to function

    DownloadableDF_tested = DownloadableDF.copy()  # make a copy so that original DF being passed does not change in memory. Good for debugging.
    ##############
    # First building an iterable to benefit from multiprocessing:
    iterable = []

    for idx in range(0, len(DownloadableDF_tested)):
        # idx=0 #delete just for testing now
        nested_url_list = [DownloadableDF_tested.iloc[idx]['HTTPServer']]
        # df_downloadable_T_S_o2.iloc[idx]['OPENDAP']]
        flattened_list = [item for sublist in nested_url_list for item in sublist]
        file_id = DownloadableDF_tested.iloc[idx]['path']
        iterable.append((flattened_list, file_id))

    ## Complete - TODO build iterator with (file_id, [flattened list of urls]) to pass to multithreading function for the whole dataset
    with (ThreadPoolExecutor(max_workers=min(32, os.cpu_count() + 4)) as executor):
        future = list(tqdm(executor.map(check_url_validity, iterable), total=len(iterable)))

    # unpacking results from generator 'future' into variable 'collector'
    collector = []
    for result in future:
        collector.append(result)

    ###############################################################
    # collector unwraps the url server handshake results in the form of :
    # collector[file_idx][0 - access url tests individually]
    # collector[file_idx][0][sub url result] - returns the tuple for that url test
    # collector[file_idx][0][sub url result][0] - returns True or False for individual url
    # collector[file_idx][0][sub url result][1] - returns individual url
    # collector[file_idx][0][sub url result][2] - returns filename
    # collector[file_idx][1 - boolean for the file if any of the links tested is_downloadable]
    ###############################################################

    # reset index of DataFrame
    DownloadableDF_tested = DownloadableDF_tested.reset_index(inplace=False, drop=False)

    state = []  # dummy array to store downloadable tests results - this will become a column in the dataframe
    # in the loop below item = collector[file_idx]
    for item in collector:
        if item[1][0] == False:  # this checks first value in collector[file_idx][1]
            print(
                f'File {os.path.basename(item[0][0][2])} not downloadable')  # look above for navigation onto the nested lists
        state.append(item[1][0])
    # DownloadableDF['state'] = state
    # appending new column to dataframe
    DownloadableDF_tested['Downloadable'] = None
    for idx, val in enumerate(state):
        DownloadableDF_tested.loc[idx, 'Downloadable'] = val

    local_paths = []  # dummy array to store local paths where results can be saved to - this will become a column in the dataframe
    path_start = 'CMIP6'
    for _ in DownloadableDF_tested['path']:
        pth = Path(path_start, *list(_.parts[3:]))  # build path only with those
        local_paths.append(pth)
    # appending new column to dataframe
    DownloadableDF_tested['local_path'] = None
    for idx, val in enumerate(local_paths):
        DownloadableDF_tested.loc[idx, 'local_path'] = val
    # DownloadableDF['local_path'] = local_paths

    return DownloadableDF_tested


def save_searched_tests(df_downloadable_tested, downloadpath):
    df_name_dummy = df_downloadable_tested['variable_id'].unique().tolist()
    df_name = '_'.join(df_name_dummy)
    fname = os.path.join(downloadpath, 'DF_Downloadable_' + df_name)
    with pd.ExcelWriter(fname + '.xlsx', engine='openpyxl') as writer:
        df_downloadable_tested.to_excel(writer)


def import_ocean_std_names(root_proj):
    for root, dirs, files in os.walk(root_proj):
        for name in files:
            if 'CMIP6_OceanVarNames.xlsx' in name:
                std_file_DF = pd.read_excel(os.path.join(root, name))
                std_names = std_file_DF['Variable Name'].to_list()
    return std_names