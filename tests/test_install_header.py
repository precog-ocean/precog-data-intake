import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__name__)))
from scripts.intake_UtilFuncs import print_precog_header

print('Behold a nice header, which is about to appear...')
print_precog_header()
print("Hooray it seems you configured things correctly")