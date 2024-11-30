
import numpy as np
import time
from distributed import Client, LocalCluster, get_client


# ---- Client funcs ------
def ensure_client(preferred_workers=12, preferred_threads=2, preferred_mem='2GB', dashboard = True):
    ''' If a function in this script that uses dask is called from a different script, 
        the client can be created or temporarily be scaled and then reverted back to the initial state '''
    def decorator(func):
        def wrapper(*args, **kwargs):
            client_exists = True
            try:
                client = get_client()
                initial_workers = len(client.cluster.workers)
                print(f'Initial client has {initial_workers} workers')
                if initial_workers == preferred_workers:  # scale client to set number of workers
                    print(f'Initial client has a suitable number of workers')     
                else:
                    print(f'Scaling number of client workers temporarily..')
                    client.cluster.scale(preferred_workers)
                    print(client)
            except ValueError:
                print(f'No initial client, so creating client')
                client_exists = False
                cluster = LocalCluster(n_workers = preferred_workers, threads_per_worker = preferred_threads, memory_limit = preferred_mem)
                client = Client(cluster)
                print(client)
                if dashboard:
                    print('opening dashboard')
                    import webbrowser
                    webbrowser.open(f'{client.dashboard_link}') 
                    # print('for dashboard, open: http://127.0.0.1:8787/status') # this is the link that is given when opening the browser from the login node
                else:
                    print(f"Dask Dashboard is available at {client.dashboard_link}")
            result = func(*args, **kwargs)
            if client_exists and initial_workers != preferred_workers:  # scale client back to what it was
                print(f'Scaling back number of workers to initial state..')
                client.cluster.scale(initial_workers)
                print(client)
            if not client_exists:
                print(f'No initial client given, so closing client')
                try:
                    client = get_client()
                    client.close()
                except:
                    print('No client defined')
            return result
        return wrapper
    return decorator







