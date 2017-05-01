#!/bin/bash

pyinstaller --noconfirm --log-level=WARN \
	--onefile --nowindow \
	--add-data="sample_data.json:." \
	parallel.py
