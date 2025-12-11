import re

import renderapi
import requests
from bigfeta.bigfeta import BigFeta


PARALLEL = 32
URL_PORT_RX = re.compile(r"(.*):(\d+)")


def montage(
    stack, pointmatch, render, clobber=False, model="rigid", schema_changes={}
):
    """perform montage with bigfeta

    stack: name of stack in render
    pointmatch: name of pointmatch in render
    render: render connection dict with keys:
        host, port, owner, project, session
    clobber: wether to error out if output stack would be overwritten
    model: transformation model to use, default "rigid",
        other options include "TranslationModel" and "SimilarityModel"
    schema_changes: override options in bigfeta schema, default {},
        takes precedence over any other parameters given

    returns the name of the created stack
    """
    z_values = [
        int(z) for z in renderapi.stack.get_z_values_for_stack(stack, **render)
    ]
    if not z_values:
        raise RuntimeError(f"no z values in stack {stack}")

    output_stack = f"{stack}_stitched"
    if clobber:
        # delete output stack if already exists
        try:
            renderapi.stack.delete_stack(output_stack, **render)
        except renderapi.errors.RenderError:
            pass

    render = {**render}  # make copy
    auth = render["session"].auth
    del render["session"]

    if "port" not in render:  # remove port from host if not present
        host = render.pop("host")
        match = URL_PORT_RX.fullmatch(host)
        if match:
            host, port = match.groups()
        elif host.startswith("https://"):
            port = 443
        else:
            port = 80

        render["host"] = host
        render["port"] = port

    # monkeypatch requests to use our auth
    old_func = requests.Session

    def session_wrapper():
        sesh = old_func()
        sesh.auth = auth
        return sesh

    requests.Session = session_wrapper
    # the reason we have to do this is because bigfeta does not offer a way to
    # set http auth, bigfeta will not connect to any other urls so this is fine

    fetaschema = {
        "close_stack": "True",
        "first_section": z_values[0],
        "last_section": z_values[-1],
        "log_level": "INFO",
        "output_mode": "stack",
        "solve_type": "montage",
        "transformation": model,
        "n_parallel_jobs": PARALLEL,
        "input_stack": {
            "name": stack,
            **render,
            "collection_type": "stack",
            "db_interface": "render",
            "use_rest": "True",
        },
        "pointmatch": {
            "name": pointmatch,
            **render,
            "collection_type": "pointmatch",
            "db_interface": "render",
        },
        "output_stack": {
            "name": output_stack,
            **render,
            "collection_type": "stack",
            "db_interface": "render",
            "use_rest": "True",
        },
        "matrix_assembly": {
            "cross_pt_weight": 1.0,
            "depth": 2,
            "inverse_dz": "True",
            "montage_pt_weight": 1.0,
            "npts_max": 500,
            "npts_min": 5,
        },
        "regularization": {
            "default_lambda": 0.005,
            "thinplate_factor": 1e-5,
            "translation_factor": 0.005,
        },
    }
    fetaschema.update(schema_changes)
    feta = BigFeta(input_data=fetaschema)
    feta.run()

    # undo monkeypatch
    requests.Session = old_func

    return output_stack
