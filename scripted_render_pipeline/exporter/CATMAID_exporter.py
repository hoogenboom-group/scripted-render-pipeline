import logging
import pathlib
from tqdm import tqdm

import numpy as np
import renderapi

import subprocess
from functools import partial
from multiprocessing import Pool

from random import sample
from renderapi.client import ArgumentParameters

from shutil import rmtree
from skimage import io, transform, img_as_ubyte

import sys
from random import sample
from ruamel.yaml import YAML
from tifffile import TiffFile
from bs4 import BeautifulSoup as Soup

class CATMAID_Exporter():
    def __init__(
        self, catmaid_dir, render, client_scripts, parallel=1, clobber=False 
    ):
        self.remote = False
        self.fmt = 'png' # Set format, standard is 'png'
        self.w_tile = 1024 # Set CATMAID tile width/height
        self.h_tile = 1024 # Standard is 1024 pixels
        self.catmaid_dir = catmaid_dir
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
        """Export render-ws project stack(s) to CATMAID format
    
        returns project info
        """
        stacks_2_export = args
        if type(stacks_2_export) is not list:
            stacks_2_export = [stacks_2_export]
        export_data = self.set_export_parameters(stacks_2_export) # Set up CATMAID export parameters
        z_values = np.unique([renderapi.stack.get_z_values_for_stack(stack,
                                                                     **self.render)\
                            for stack in stacks_2_export])
        logging.info(
            f"Running render_catmaid_boxes..."
        )
        print("run boxClient")
        self.render_catmaid_boxes_across_N_cores(stacks_2_export, export_data, z_values)
        print("completed")
        logging.info(
            f"Done"
            f"Resorting tiles..."
        )
        self.resort_tiles(stacks_2_export, z_values)
        logging.info(
            f"Making thumbnails..."
        )
        self.make_thumbnails(stacks_2_export, z_values)
        logging.info(
            f"Making project file..."
        )
        project_yaml, project_data = self.create_project_file(stacks_2_export, export_data)
        out = f"""\
        {project_yaml}
        --------\
        """
        print(out)
        
    def set_export_parameters(self, stacks_2_export) -> list: 
        # Initialize collection for export parameters
        export_data = {}
        # Update max level
        maxest_level = 0
        # Iterate through stacks
        for stack in stacks_2_export:
            # Determine `max_level` such that the full section is in view when fully zoomed out
            stack_bounds = renderapi.stack.get_stack_bounds(stack=stack,
                                                            **self.render)
            w_stack = max(stack_bounds['maxX'] - stack_bounds['minX'],
                        stack_bounds['maxY'] - stack_bounds['minY'])
            max_level = int(np.ceil(np.log(w_stack / self.w_tile) * 1/np.log(2)))
            # Export each stack to highest level in the project
            maxest_level = max(max_level, maxest_level)
            # Set parameters for export to CATMAID
            export_params = CatmaidBoxesParameters(stack=stack,
                                                   root_directory=self.catmaid_dir.parent.as_posix(),
                                                   width=self.w_tile,
                                                   height=self.h_tile,
                                                   fmt=self.fmt,
                                                   max_level=maxest_level,
                                                   localhost='http://sonic.tnw.tudelft.nl',
                                                   port=8081,
                                                   **self.render)
            # Add CATMAID export parameters to collection
            export_data[stack] = export_params
            return export_data
     
    def render_catmaid_boxes_across_N_cores(self, stacks_2_export, export_data, z_values): # override 
        # Path to `render_catmaid_boxes` shell script
        fp_client = pathlib.Path(self.client_scripts) / 'render_catmaid_boxes.sh'
        # Set number of cores for multiprocessing
        N_cores = min(15, len(z_values))
        # Iterate through stacks to export
        for stack in tqdm(stacks_2_export):
            # Create java arguments from export parameters
            java_args = list(export_data[stack].to_java_args())
            # Set up `BoxClient` client script call
            call_run_ws_client_partial = partial(self.call_run_ws_client,
                                                 className='org.janelia.render.client.BoxClient',
                                                 java_args=java_args)
            # render_catmaid_boxes_partial = partial(self.render_catmaid_boxes,
            #                                        client_script=fp_client,
            #                                        java_args=java_args)
            
            # Run `render_catmaid_boxes` across `N_cores`
            with Pool(N_cores) as pool:
                pool.map(call_run_ws_client_partial, z_values)
                # pool.map(render_catmaid_boxes_partial, z_values)

    def call_run_ws_client(self, z, className, java_args):
            """Wrapper for `call_run_ws_client` script to enable multiprocessing"""
            _args = [f'{z:.0f}'] + java_args # specify z-level
            print(_args)
            # Call "render-ws-client"
            renderapi.client.client_calls.call_run_ws_client(className,
                                                             add_args=_args,
                                                             memGB='2G',
                                                             client_script="/home/catmaid/render/render-ws-java-client/src/main/scripts/run_ws_client.sh",
                                                             )

    def render_catmaid_boxes(self, z, client_script, java_args):
            """Wrapper for `render_catmaid_boxes` script to enable multiprocessing"""
            p = subprocess.run([client_script.as_posix(), f'{z:.0f}'] + java_args)
    
    def resort_tiles(self, stacks_2_export, z_values): 
        # Iterate through stacks to export
        for stack in tqdm(stacks_2_export):
            # Loop through all the exported tiles per stack
            fps = (self.catmaid_dir / stack).glob(f"{self.w_tile}x{self.h_tile}/**/[0-9]*.{self.fmt}")
            for fp in fps:
                # Extract tile info from filepath
                zoom_level = int(fp.parents[2].name)
                z = int(fp.parents[1].name) - int(z_values.min())  # 0-index
                row = int(fp.parents[0].name)
                col = int(fp.stem)
                # Reformat tile
                tile_format_1 = self.catmaid_dir / stack / f"{z}/{row}_{col}_{zoom_level}.{self.fmt}"
                tile_format_1.parent.mkdir(parents=True, exist_ok=True)
                fp.rename(tile_format_1)
            # Clean up (now presumably empty) directory tree
            rmtree((self.catmaid_dir / stack / f"{self.w_tile}x{self.h_tile}").as_posix())
    
    def make_thumbnails(self, stacks_2_export, z_values):
        # Loop through stacks to export
        for stack in tqdm(stacks_2_export):
            # Loop through each section
            for z in (z_values - z_values.min()):
                # Load most zoomed out image (0, 0, `max_level`)
                fp = max(self.catmaid_dir.glob(f"{stack}/{z:.0f}/0_0_*.{self.fmt}"))
                zoom = int(fp.stem[-1])
                image = io.imread(fp)
                # Resize
                bounds = renderapi.stack.get_stack_bounds(stack=stack,
                                                          **self.render)
                width_ds = bounds['maxX'] - bounds['minX']  # width of dataset at zoom level 0
                width_rs = (192 / (width_ds/2**zoom)) * self.w_tile
                image_rs = transform.resize(image, output_shape=(width_rs, width_rs))
                # Crop to content
                thumb = image_rs[np.ix_((image_rs > 0).any(1), (image_rs > 0).any(0))]
                thumb_rs = transform.resize(thumb, output_shape=(192, 192))
                # Save
                fp_thumb = self.catmaid_dir / f"{stack}/{z:.0f}/small.{self.fmt}"
                io.imsave(fp_thumb, img_as_ubyte(thumb_rs))

    def create_project_file(self, stacks_2_export, export_data):
        # Set project yaml file
        project_yaml = self.catmaid_dir / 'project.yaml'
        # Collect stack data
        stack_data = []
        for stack in tqdm(stacks_2_export):
            # Get dimension data
            bounds = renderapi.stack.get_stack_bounds(stack=stack,
                                                      **self.render)
            dimensions = (int((bounds['maxX'] - bounds['minX']) * 1.1),
                        int((bounds['maxY'] - bounds['minY']) * 1.1),
                        int(bounds['maxZ'] - bounds['minZ'] + 1))
            # Get resolution data 
            stack_metadata = renderapi.stack.get_full_stack_metadata(stack=stack,
                                                                     **self.render)
            resolution = (np.round(1e3*stack_metadata['currentVersion']['stackResolutionX'], 5),
                        np.round(1e3*stack_metadata['currentVersion']['stackResolutionY'], 5),
                        np.round(stack_metadata['currentVersion']['stackResolutionZ'], 5))
            # Get metadata
            ts = sample(renderapi.tilespec.get_tile_specs_from_stack(stack=stack,
                                                                     **self.render), 1)[0]
            fp = ts.ip[0]['imageUrl']
            tif = TiffFile(fp)
            metadata = tif.pages[0].description
            export_data_list = list(export_data[stack].to_java_args())
            maxest_level = export_data_list.index("--maxLevel") + 1 # Specific list element

            # Project data for output to project yaml file
            stack_datum = {
                "title": f"{stack}",
                "dimension": f"{dimensions}",
                "resolution": f"{resolution}",
                "zoomlevels": f"{(maxest_level + 1):.0f}",
                "metadata": metadata,
                "mirrors": [{
                    "title": f"{self.project}_{stack.split('_')[0]}",
                    "tile_width": self.w_tile,
                    "tile_height": self.h_tile,
                    "tile_source_type": 1,
                    "fileextension": f"{self.fmt}",
                    "url": f"https://sonic.tnw.tudelft.nl{(self.catmaid_dir / stack).as_posix()}/"
                }]
            }
            stack_data.append(stack_datum)

        # Create dict for input into project yaml file
        project_data = {
            "project": {
                "title": f"{self.project}",
                "stacks": stack_data
            }
        }

        yaml = YAML()
        yaml.indent(mapping=2, offset=0)
        yaml.dump(project_data, project_yaml)
        yaml.dump(project_data, sys.stdout)
        return project_yaml, project_data
class CatmaidBoxesParameters(ArgumentParameters):
    """Subclass of `ArgumentParameters` for facilitating CATMAID export client script"""
    def __init__(self, stack, root_directory,
                 width=1024, height=1024, fmt='png', max_level=0, 
                 localhost=None, port=None, **kwargs):

        super(CatmaidBoxesParameters, self).__init__(**kwargs)

        self.stack = stack
        self.rootDirectory = root_directory
        self.height = height
        self.width = width
        self.format = fmt
        self.maxLevel = max_level
        self.baseDataUrl = renderapi.render.format_baseurl(host=localhost,
                                                           port=port)
        self.owner = kwargs["owner"]
        self.project = kwargs["project"]


