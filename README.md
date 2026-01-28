# precog-data-intake 🦑

Data sweep and scraping utilities for downloading and organizing Earth system data from Earth System Grid Federation ([ESGF](https://esgf-ui.ceda.ac.uk/search)) nodes based on core functionality from [esgf-intake](https://intake-esgf.readthedocs.io/en/latest/)

## Overview

This repository contains scripts and workflows to automate bulk discovery, file checking, validation and download of datasets from ESGF archives.
It is designed to support climate and oceanographic analyses that require consistent, reproducible access to large model and observational datasets.

## Features

- Automated search through the ESGF catalogue using project, variable, experiment, and temporal filters.
- Verification of continuity of date stamps in CMIP6 Pre-Industrial (PI) and Historical runs.
- Verification and logging of availability of (PI) and Historical runs on consistent grids (e.g., 'gr' and 'gn'). 
- Export of simple Dataframes for realized ESGF catalogue searches.
- Verification of server responses and flagging shortlisted ESM outputs as 'Downloadable'.
- Combined conditional search for availability of PI and Historical runs in CMIP6 models across >1 variable (e.g., 'expc' & 'epc100').
- Parallelized URL checking for fastest connection in case same data are available across different nodes.
- Batched download of files from multiple ESGF nodes with retry and integrity checks.
- Local directory layout optimized for downstream analysis tools.
- Batched search and download of grid cell measures (e.g., 'areacello' and 'volcello') with archive snapshot of relaxed regex matches for CMIP6 models of interest.

## Installation

These instructions assume you have **Python 3.12+** installed and available as `python` or `python3` on your system.

### 1. Clone the repository

```bash
git clone https://github.com/LeoBertini/precog-data-intake.git
cd precog-data-intake
````
### 2. Create a virtual environment (named .venv)
```bash
python3 -m venv .venv
````
Make sure your python version is >3.12.
You can check which python you have installed on your machine by typing `python` followed by `TAB` a few times. It will list your python versions.

The argument '.venv' means a hidden directory called `'.venv'` under the project directory you are in (i.e., `~/precog-data-intake/.venv`). This is where the virtual environment called 'venv' will be created with its dependencies put in the `/.venv` directory. 

### 3. Activate it
```bash
source .venv/bin/activate
```
When activation succeeds, your shell prompt will usually show the environment name in parentheses, for example: `(.venv) ...`

### 4. Upgrade pip (recommended)
Inside the activated virtual environment, upgrade `pip`:
```bash
python -m pip install --upgrade pip
```
### 5. Install dependencies
```bash 
pip install -r requirements.txt
```

## Repository structure
```                

precog-data-intake/
├─.venv/                 # Hidden folder with virtual enviroment dependencies               
├─ intake_esgf_mods/     # Modified Python modules from the main package "esgf-intake"
├─ misc_images/          # Images (these are renderered as ASCII art headers and footers in the CLI)
├─ scripts/              # Workflows as command line scripts
├─ notebooks/            # Example notebooks of usage
└─ tests/                # Basic tests for core functionality
```

## Usage

Make sure that you have activated the python virtual environment. Then run:

```bash 
python3 tests/test_install_hearder.py
```
If you see the following being printed on the terminal, you have installed things correctly.

```bash

         5PO8$HHDBWWWWBBHK@UOPp                                                                                         
     !P&KWN0QQQQQQQ000QQQQQQ0NWHUGn                                                                                     
   FAHMQQQ0000000000000RR00000QQQ0D&g                                                                                   
 IbBQQ00000000000000Q0000R00000QXPAUY4gg4SShc  '2SVgggg4SSh%  ;pSdgggggggg>  +mS4ggggggu   7ghggggggSg7   Lghggggggggt  
jAQQR0000000000000Q0000RRR00000HnMQPezjz3QQN/  aQQG77zz3QQW+  jQQEo77777jj,  TQQELLLLLLx  <BQNnLLLnNQQI  %MQDuLj7777j>  
EQ0RRR0RRMNMMRRRRR00Q0Q000000RQ2ZQQ4T#ywEQQj  ,OQQVTJyJ4QQz  +$QQST#CCCCv   \HQD^         FQQj    *0Q4   gQQ1  "Lzzj)   
XWDDDHDBDWWNWBWBBDHKHDDDBHDDDWkJQQV}?1[1I*{-  TQQh}?!7QQW?-  fQQF{*}***}=   5QQj         lRQA`   ,OQR)  I0QZ   )aQQQ>   
fGK@KgEHdOK@K4GK@KkpV@$$Uq8@U$TORD| 1Fmw     |$0D/   |N0@_  )H00dppmmmmF+  xW0RVqhGPGz  -SQ0AmqPPXQ0F  _E00kmqGX400w    
IpgVqd44dd44ddd4d6qd44hmhd44gge*cv,|h4qC     =v)|    ;v><.  /v))%xx%xx%v.  "v))%x%%%%^  -v)>vxx%%%))'  'v))vxx%%%)>_    
 lLwy3yyJyyy#CyyywfyyyywfyyyyJyCJ#C3y#?                                                                                 
   t7jn#nTTnTnnnLTnLunnnjunnnnT#JTLje                                                                                   
     )zLLunTT#nTTTnnnTTTunT#TunuLL7                                                                                     
         ejojzLununnnnuuuuuLozz```