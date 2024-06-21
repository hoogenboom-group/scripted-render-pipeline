# scripted render pipeline
Automated Python pipeline for processing volume electron microscopy (EM) and integrated correlative light and electron microscopy (CLEM) datasets, interfacing with the [render-ws](https://github.com/saalfeldlab/render/blob/master/docs/src/site/markdown/render-ws.md) rest api. `scripted-render-pipeline` is configured to process (correlative) array tomography datasets in formats from [FAST-EM](https://www.delmic.com/en/products/fast-imaging/fast-em), a multibeam scanning transmission electron microscope, and [SECOM](https://www.delmic.com/en/products/clem-solutions/secom), an optical microscope for integrated correlative light and electron microscopy.

The following is currently supported:
- Automated post-correction (FAST-EM datasets only)
- Import to render-ws (FAST-EM and SECOM datasets)
- Export to (self-managed) [WebKnossos](https://webknossos.org/) instances (FAST-EM and SECOM datasets).

This repository is tied to the [interactive render workflow](https://github.com/hoogenboom-group/interactive-render-workflow), which covers (supervised) 2D stitching and 3D alignment of FAST-EM data. The modules in the `scripted-render-pipeline` can be used in the `interactive-render-workflow`.

### Installation 
This instruction assumes that `git` and Python are installed. Moreover, it is recommended to install the software in a Python virtual environment. Python 3.10 and later versions are supported.
Clone the repository into a suitable target directory and install with `pip`:
```
git clone https://github.com/hoogenboom-group/scripted-render-pipeline
pip install --require-virtualenv .
```
this will install required dependencies from PyPI as well

### Usage
`render_import`  for importing to render

`render_export`  for exporting to CATMAID or WebKnossos

`render_basic_auth {show,save}`  for managing http basic auth credentials

`post_correct`  for post-corrrecting FAST-EM images (prior to import)

Each module has a `main.py` file which executes the code when called from the command line using the commands listed above. Dataset and processing parameters can be set in this file. 

### usage as module
`python -m scripted_render_pipeline.importer`  for importing to render

`python -m scripted_render_pipeline.basic_auth {show,save}`  for managing http basic auth credentials

`python -m scripted_render_pipeline.exporter` for exporting to WebKnossos
