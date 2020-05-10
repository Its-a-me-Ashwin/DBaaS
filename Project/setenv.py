import os

# Set environment variables
os.environ['TYPE'] = 'MASTER'

for i in os.environ :
    print(i,os.environ[i])
