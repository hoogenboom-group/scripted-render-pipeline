# scripted render pipeline
automated pipeline interfacing with render-ws rest api

### installation
```
pip install --require-virtualenv .
```
this will install required dependencies from PyPI as well

### usage
`render_import`  for importing to render

`render_export`  for exporting to CATMAID or WebKnossos

`render_basic_auth {show,save}`  for managing http basic auth credentials

`post_correct`  for post-corrrecting FAST-EM images (prior to import)

### usage as module
`python -m scripted_render_pipeline.importer`  for importing to render

`python -m scripted_render_pipeline.basic_auth {show,save}`  for managing http basic auth credentials
