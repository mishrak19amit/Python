import webbrowser
import shutil
import datetime
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

dirname = config['FILE_DOWNLOAD']['DIR_NAME']
downlablefilename= config['FILE_DOWNLOAD']['GOOGLE_FILE_LINK_PATH']

print(dirname)
print(downlablefilename)

x = datetime.datetime.now()
print(x)

try:
    shutil.rmtree(dirname)
    print("Directory has been cleaned")
except Exception:
    print("Directory is already Empty")

f = open(downlablefilename, "r")
for x in f:
    #print(x)
    webbrowser.open(x)

x = datetime.datetime.now()
print(x)