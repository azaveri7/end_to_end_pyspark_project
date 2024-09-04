import os
import pprint as pp

#pp.pprint(dict(os.environ)['JAVA_HOME'])

### Set environment variables
os.environ['envn'] = 'TEST'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'
appName = "USA Prescriber Research Report"
current_path = os.getcwd()
staging_dim_city = os.path.normpath(os.path.join(current_path, '..', 'staging', 'dimension_city'))
staging_fact = os.path.normpath(os.path.join(current_path, '..', 'staging', 'fact'))

### Get Environment variables
envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

