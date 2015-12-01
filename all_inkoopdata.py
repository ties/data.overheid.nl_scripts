import ckanapi
import requests
import urllib

api = ckanapi.RemoteCKAN('https://data.overheid.nl/data')

def download_package_resource_files(package):
    for resource in package['resources']:
        url = resource['url']
        url_tokens = urllib.parse.urlparse(url)

        file_name = url_tokens.path.split('/')[-1]

        # data.overheid.nl uses Strict-Transport-Security but the resource URL
        # points to http, which gets redirected to https://data.overheid.nl/$
        # (it loses the path)
        #
        # Workaround:
        # Mangle the URL to use https
        #
        url_tokens = list(url_tokens)
        url_tokens[0] = 'https'

        url = urllib.parse.urlunparse(url_tokens)
        print(url)

        download_file(url, file_name)

def download_file(url, local_filename):
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=512*1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                #f.flush() commented by recommendation from J.F.Sebastian
    return local_filename
    


spending = [p for p in api.action.package_list() if p.startswith('inkoopdata')]

packages = [api.action.package_show(id=i) for i in spending]

# Get all the spending files
for package in package:
    download_package_resource_files(package)
