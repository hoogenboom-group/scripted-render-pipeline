import logging
import pathlib
from turtle import st
from typing import cast

import numpy as np
import renderapi
import subprocess
import os
from ruamel.yaml import YAML

from .CATMAID_exporter import CATMAID_Exporter

# Haven't found a smart way yet to directly write data to WK format. Problem is that the render client writes data to disk
# We want to write the data from an array to WK format
# Work around is to export data to CATMAID format, then call the wk-cuber script to convert this into a WebKnossos data set


class WK_Exporter():
    def __init__(
        self, wk_dir, catmaid_dir: pathlib.Path, render, client_scripts: str | pathlib.Path,
        wk_client_script: str | pathlib.Path, parallel=1,
        clobber: bool = False, remove_CATMAID_dir: bool = False,
    ):
        self.remote = False
        self.fmt = 'png'  # Set format, standard is 'png'
        self.w_tile = 1024  # Set tile width/height
        self.h_tile = 1024  # Standard is 1024 pixels
        self.wk_dir = wk_dir  # UNUSED
        self.catmaid_dir = catmaid_dir
        self.wk_client_script = wk_client_script
        self.parallel = parallel  # UNUSED
        self.clobber = clobber
        self.remove_catmaid_dir = remove_CATMAID_dir
        self.render = render  # render connect object

        self.host = render["host"]
        self.owner = render["owner"]
        self.project = render["project"]
        self.client_scripts = client_scripts

    # TODO: This seems to never be used, and it uses non-existent class attributes, should be removed?
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

    def export_stacks(self, args: list[str] | str) -> None:
        """Export render-ws project stack(s) to WebKnossos data format

        returns project info
        """
        stacks_2_export = args
        if type(stacks_2_export) is str:
            stacks_2_export = [stacks_2_export]
        try:
            no_stacks = len(stacks_2_export)
            if no_stacks > 1:
                raise MoreThanOneStack
        except MoreThanOneStack:
            # TODO: remove this error once I have confirmed that the code works for multiple stacks
            print('Exporting more than one stack to WebKnossos is not supported')

        # Tell type checker we are sure that stacks_2_export is a list of strings now
        stacks_2_export = cast(list[str], stacks_2_export)

        # Check if catmaid_dir exists, if yes go directly to WK conversion
        if not os.path.isdir(self.catmaid_dir / stacks_2_export[0]):
            # Create CATMAID_exporter class instance
            CATMAID_exporter = CATMAID_Exporter(self.catmaid_dir, self.render, self.client_scripts,
                                                self.parallel, self.clobber)
            export_data = CATMAID_exporter.set_export_parameters(
                stacks_2_export)  # Set up CATMAID export parameters
            z_values = np.unique([renderapi.stack.get_z_values_for_stack(stack,
                                                                         **self.render)
                                  for stack in stacks_2_export])
            # Render tiles with BoxClient
            logging.info(
                "Running BoxClient..."
            )
            CATMAID_exporter.render_catmaid_boxes_across_N_cores(
                stacks_2_export, export_data, z_values)
            print("completed")
            # Resort tiles into preferred format
            logging.info(
                "Done"
                "Resorting tiles..."
            )
            CATMAID_exporter.resort_tiles(stacks_2_export, z_values)
            CATMAID_exporter.make_thumbnails(stacks_2_export, z_values)
            logging.info(
                "Making project file..."
            )
            _, project_data = CATMAID_exporter.create_project_file(
                stacks_2_export, export_data)
        else:
            yaml = YAML()
            project_data = yaml.load(self.catmaid_dir / 'project.yaml')
            # ensure all stacks are in the project file
            # FIXME: If you have iteratively exported the stacks instead of in one go, the project.yaml file might not contain all stacks
            # This is a problem that should probably be fixed in the CATMAID_exporter class
            for stack in stacks_2_export:
                if not any(stack == s["title"] for s in project_data['project']['stacks']):
                    # TODO: run CATMAID_exporter.export_stacks(stacks_2_export) instead?
                    # this might be a bit of a hack, but it should work as it will just skip the exports that already exist I think
                    raise ValueError(f"stack {stack} not found in project file")

        for stack in stacks_2_export:
            # get the stack info from the project_data
            stack_info = next((s for s in project_data['project']['stacks'] if s["title"] == stack))

            # Extract voxel size
            voxel_size = stack_info['resolution'].strip("()").split(",")
            voxel_size = tuple(map(float, voxel_size))
            voxel_size = tuple(map(int, voxel_size))

            # Call WebKnossos conversion script
            logging.info(
                "Converting %s to .wk format...", stack)
            self.call_wk_conversion_script(
                stack, layer_name=stack, voxel_size=voxel_size)
            logging.info(
                "Conversion done...")

    def call_wk_conversion_script(self, stack_2_export: str, layer_name: str = "color",
                                  voxel_size: tuple[int, int, int] = (4, 4, 90)) -> None:
        """Wrapper for CATMAID to WK format conversion script

        returns nothing
        """
        try:
            # Run the command
            subprocess.run([f"{self.wk_client_script}", f"{self.catmaid_dir}/{stack_2_export}", f"{self.project}",
                            f"{layer_name}", f"{voxel_size[0]},{voxel_size[1]},{voxel_size[2]}"],
                           check=False  # Explicitly set to False so linter stops complaining
                           )
        except subprocess.CalledProcessError as e:
            print(f"Error: {e}")

        # (Optionally) remove CATMAID directory because it has become obsolete
        if self.remove_catmaid_dir:
            try:
                os.rmdir(self.catmaid_dir)
            except:
                print('Error deleting CATMAID directory')

# define Python user-defined exceptions


class MoreThanOneStack(Exception):
    "Raised when the input value is less than 18"
    pass
