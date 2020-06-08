import requests


def get_and_save_data(url, filename):
    """Given a url and a filename, gets the data from that URL (must be a CSV)
    and saves it to the given filename.
    """
    r = requests.get(url)
    if r.status_code == 200:
        with open(filename, 'w') as f:
            f.writelines(r.text)
        return True
    else:
        raise IOError('Error getting data: {}'.format(r.reason))
