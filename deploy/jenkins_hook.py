from subprocess import *
from flask import Flask, request

app = Flask(__name__)


def check_output_and_error(*popen_args, **kwargs):
    if 'stdout' in kwargs or 'stderr' in kwargs:
        raise ValueError('stdout and stderr argument not allowed,'
                         ' it will be overridden.')
    process = Popen(stdout=PIPE, stderr=PIPE, *popen_args, **kwargs)
    output, err = process.communicate()
    return_code = process.poll()
    if return_code:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popen_args[0]
        raise CalledProcessError(return_code, cmd, output=output + '\n\n' + err)
    if err.strip():
        output += ('\n\n' + err + '\n\n')
    return output


@app.route("/deploy", methods=['POST'])
def deploy():
    tag = request.args.get('tag', 'HEAD')
    deploy_script = [
        ["bash", "deploy/deploy.bash"],
        ["service", "neo4j", "restart"],
    ]
    params = dict(cwd="/home/dmp/dmp-graph")

    git = [
        ["git", "fetch", "origin"],
        ["git", "fetch", "--tags", "origin"],
        ["git", "checkout", tag]
    ]

    return_code = 0
    output = ''

    try:
        for cmd in git + deploy_script:
            output += check_output_and_error(cmd, **params)
    except CalledProcessError as e:
        output = e.message + '\n\n' + e.output
        return_code = e.returncode

    if return_code != 0:
        return output, 400, {'X-ReturnCode': return_code}

    return output, 200


if __name__ == "__main__":
    import sys
    app.run(host='0.0.0.0', port=4747, debug='-d' in sys.argv)
