import logging
import pathlib

import numpy as np
import renderapi
import subprocess
import os

from .CATMAID_exporter import CATMAID_Exporter

# Haven't found a smart way yet to directly write data to WK format. Problem is that the render client writes data to disk
# We want to write the data from an array to WK format
# Work around is to export data to CATMAID format, then call the wk-cuber script to convert this into a WebKnossos data set

class WK_Exporter():
    def __init__(
        self, wk_dir, catmaid_dir, render, client_scripts, 
        wk_client_script, parallel=1, clobber=False 
    ):
        self.remote = False
        self.fmt = 'png' # Set format, standard is 'png'
        self.w_tile = 1024 # Set tile width/height
        self.h_tile = 1024 # Standard is 1024 pixels
        self.wk_dir = wk_dir
        self.catmaid_dir = catmaid_dir
        self.wk_client_script = wk_client_script
        self.parallel = parallel
        self.clobber = clobber
        self.render = render # render connect object

        self.host = render["host"]
        self.owner = render["owner"]
        self.project = render["project"]
        self.client_scripts = client_scripts
        
    def to_server_path(self, path: pathlib.Path) -> str:
        """convert a local path to the location on the server

        path:
            an absolute path on this machine that is mapped to a server
            location
        returns a posix format server path as string
        """
        if not self.remote:
            return path.as_posix()

        total_parts = len(self.nas_share_path.parts)
        if path.parts[:total_parts] != self.nas_share_path.parts:
            raise ValueError(f"path {path} is not on the share")

        return self.server_storage_path_str + "/".join(
            path.parts[total_parts:]
        )

    def export_stacks(self, args):
        """Export render-ws project stack(s) to WebKnossos data format
    
        returns project info
        """
        stacks_2_export = args
        if type(stacks_2_export) is not list:
            stacks_2_export = [stacks_2_export]
        # Check if catmaid_dir exists, if yes go directly to WK conversion
        if not os.path.isdir(self.catmaid_dir):
            # Create CATMAID_exporter class instance 
            CATMAID_exporter = CATMAID_Exporter(self.catmaid_dir, self.render, self.client_scripts, 
                                                self.parallel, self.clobber)
            export_data = self.set_export_parameters(stacks_2_export) # Set up CATMAID export parameters
            z_values = np.unique([renderapi.stack.get_z_values_for_stack(stack,
                                                                        **self.render)\
                                for stack in stacks_2_export])
            # Render tiles with BoxClient
            logging.info(
                f"Running BoxClient..."
            )
            CATMAID_exporter.render_catmaid_boxes_across_N_cores(stacks_2_export, export_data, z_values)
            print("completed")
            # Resort tiles into preferred format
            logging.info(
                f"Done"
                f"Resorting tiles..."
            )
            CATMAID_exporter.resort_tiles(stacks_2_export, z_values)
            # Call WebKnossos conversion script
            logging.info(
                f"Converting to .wk format...")
        self.call_wk_conversion_script()
        logging.info(
            f"Conversion done...")

    def call_wk_conversion_script(self, remove_CATMAID_dir=False):
        """Simple call of CATMAID to WK format conversion script via shell

        returns nothing
        """
        # Virtual environment
        virtualenv_activate = '/opt/webknossos/tool/bin/activate'

        # Call WK shell script in CATMAID project directory
        shell_command = [self.wk_client_script, str(self.catmaid_dir)]
        try:
            # Use 'source' to activate the virtual environment
            activation_cmd = f'. {virtualenv_activate} && {shell_command}'
            # Run the command
            subprocess.run(activation_cmd, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error: {e}")

        # (Optionally) remove CATMAID directory because it has become obsolete
        if remove_CATMAID_dir:
            try:
                os.rmdir(self.catmaid_dir)
            except:
                print('Error deleting CATMAID directory')

