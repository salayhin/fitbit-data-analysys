#!/usr/bin/env bash

cd /export/sc-ehealth01/fitbit

#Step 3: Enable Virtualenv
source venv/bin/activate

#Step 4: Go to Jupiter folder
cd fitbit-data-analysys

#Step 5: Run Jupyter Notebook
python fitbit_data_import.py
