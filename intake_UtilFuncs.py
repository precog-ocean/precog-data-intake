import os
import pandas as pd
import time
import logging
import requests
from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def append_cols(PandasDataFrame, new_col_names, positional_order):
    for col, idx in zip(new_col_names, positional_order):
        print(f"Extracting info from dataset_id the following facet: {col} which appears as argument number: {idx}")
        PandasDataFrame[[col]] = PandasDataFrame["dataset_id"].apply(lambda x: pd.Series(str(x).split(".")[idx]))

    return PandasDataFrame  # this is the dataframe with appended columns that make it easier to sort and cross-check across models and variables


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
    date_strs_start = DataFrameSubset['file_start'].reset_index(drop=True)
    date_strs_end = DataFrameSubset['file_end'].reset_index(drop=True)

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

    return counter, single_file, consecutive_test[
                                 :-1]  # removes the last boolean of consecutive_test because this involves the padded test


def catalog_traverser(logger, CatalogDF, varlist):
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

        variables_in = check_var_in(df1_subset,
                                    varlist)  # checks if any of the variables returns an empty dataframe, if they all return entries then proceed

        if not all(variables_in):
            logger.info(
                f'The model: {model} does not have all variables of interest: test for {varlist} returned {variables_in} \n')
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

                        if df1_pi['variant_label'].unique() == df1_historical['variant_label'].unique():
                            logger.info(
                                f'Variant labels {df1_pi['variant_label'].unique()} from piCrontrol and Historical in model {model} are matching')

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
                    f'No complete set of variables for model {model} for variable {var} in either grid. Test returned:')
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
        vart_test_ordered = set(var_test)
        varlist_ordered = set(varlist)

        if vart_test_ordered == varlist_ordered:  # test if model has a grid output across all variables of interest
            logger.info(
                f'Model {model} has all outputs for variables {varlist_ordered} in a {grid} grid for the {run} run')

            catch_state = True
            var_test_final = [i == j for i, j in zip(varlist_ordered, vart_test_ordered)]

        else:
            missing_var = varlist_ordered - vart_test_ordered
            logger.info(
                f'Model {model} does not have all outputs for variables {varlist} in a {grid} grid for the {run} run')
            logger.info(f'Test returned the following vars only {vart_test_ordered}')
            logger.info(f'Variable {missing_var} is missing. Ignoring model outputs for grid {grid}.\n')

            # padding for comparission between lists
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

        dict['var_test'].append(var_test_final)
        dict['run'].append(run)
        dict['has_all_variables'].append(catch_state)
        dict['model'].append(model)
        dict['variable_ids'].append(varlist_ordered)
        dict['grid_label'].append(grid)

    return dict

    # some initial parameters to be set so that the seach happens across all nodes and the cached files can be downloaded to a custom directory


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

    ##############
    # First building an iterable to benefit from multiprocessing:
    iterable = []

    for idx in range(0, len(DownloadableDF)):
        # idx=0 #delete just for testing now
        nested_url_list = [DownloadableDF.iloc[idx]['HTTPServer']]  #
        # df_downloadable_T_S_o2.iloc[idx]['OPENDAP']]
        flattened_list = [item for sublist in nested_url_list for item in sublist]
        file_id = DownloadableDF.iloc[idx]['path']
        iterable.append((flattened_list, file_id))

    ## Complete - TODO build iterator with (file_id, [flattenend list of urls]) to pass to multithreading function for the whole dataset
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

    state = []  # dummy array to store downloadable tests results - this will become a column in the dataframe
    for item in collector:
        if item[1][0] == False:
            print(
                f'File {os.path.basename(item[0][0][2])} not downloadable')  # look above for navigation onto the nested lists
        state.append(item[1][0])
    DownloadableDF['Downloadable'] = state

    local_paths = []  # dummy array to store local paths where results can be saved to - this will become a column in the dataframe
    path_start = 'CMIP6'
    for _ in DownloadableDF['path']:
        pth = Path(path_start, *list(_.parts[3:]))  # build path only with those
        local_paths.append(pth)
    DownloadableDF['local_path'] = local_paths

    return DownloadableDF


def save_searched_tests(df_downloadable_tested, downloadpath):
    df_name_dummy = df_downloadable_tested['variable_id'].unique().tolist()
    df_name = '_'.join(df_name_dummy)
    fname = os.path.join(downloadpath, 'DF_Downloadable_' + df_name)
    with pd.ExcelWriter(fname + '.xlsx', engine='openpyxl') as writer:
        df_downloadable_tested.to_excel(writer)
