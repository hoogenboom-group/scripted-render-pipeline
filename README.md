# scripted render pipeline
Automated Python pipeline for processing volume electron microscopy (EM) and integrated correlative light and electron microscopy (CLEM) datasets, interfacing with the render-ws rest api. `scripted-render-pipeline` is configured to process (correlative) array tomography datasets in formats from FAST-EM, a multibeam scanning transmission electron microscope, and SECOM, an optical microscope for integrated correlative light and electron microscopy.

The software supports automated post-correction of FAST-EM datasets, import to render-ws of volume EM and CLEM datasets and export to (self-managed) WebKnossos instances.

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

### usage as module
`python -m scripted_render_pipeline.importer`  for importing to render

`python -m scripted_render_pipeline.basic_auth {show,save}`  for managing http basic auth credentials
