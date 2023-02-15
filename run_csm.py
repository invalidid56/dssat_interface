import datetime
import shutil
import subprocess
import os
import re
from pydantic import BaseModel
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from enum import Enum
from typing import Optional, Tuple, List
from components import get_weather


#
# Define Env const
# Todo: to env variable
WTH_PATH = os.getcwd()
SOL_PATH = 'CSM'
X_PATH = 'CSM'




def run_csm(xfile):
    #
    # Check Farmland by Coordinate
    #
    # farmland = FarmLand.SAMPLE

    #
    # Make Temp Dir.
    #
    if os.path.exists('temp'):
        shutil.rmtree('temp')
    os.mkdir('temp')

    #
    # Create Farmland's Weather DATA
    #
    whether_code = 'JEXU'
    whether_files = [x for x in os.listdir(os.getcwd()) if x.startswith(whether_code)]
    for wth in whether_files:
        shutil.copy(os.path.join(WTH_PATH, wth), os.path.join('temp', wth))

    #
    # Copy Farmland's SOL file to pwd
    #
    soil_code = 'SI'
    soil_files = [x for x in os.listdir(SOL_PATH) if x.startswith(soil_code)]
    for sol in soil_files:
        shutil.copy(os.path.join(SOL_PATH, sol), os.path.join('temp', sol))

    #
    # Copy Crop's Sample X File to pwd
    #
    crop_code: str = 'bs'
    x_file = [x for x in os.listdir(X_PATH) if x.endswith(crop_code.upper() + 'X')][0]

    shutil.copy(os.path.join(X_PATH, x_file), os.path.join('temp', x_file))

    #
    # Run CSM
    #
    subprocess.call('docker run --rm -v {0}/temp:/data -w /data dssat-csm A {1}'.format(
        os.getcwd(),
        xfile
    ), shell=True)

    return True


print(run_csm('BU019701.BSX'))