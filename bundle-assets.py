import shutil
from flask import Flask
from flask_assets import Environment, Bundle

app = Flask(__name__, root_path='fedsdm/')
assets = Environment(app)

NPM_PATH = '../../node_modules/'
JS_PATH = 'js/libs/'
CSS_PATH = 'css/'
WEBFONTS_PATH = 'webfonts/'

bundles = {
    'bootstrap_js': Bundle(
        NPM_PATH + 'bootstrap/dist/js/bootstrap.min.js',
        filters='jsmin',
        output=JS_PATH + 'bootstrap.min.js'
    ),
    'bootstrap_css': Bundle(
        NPM_PATH + 'bootstrap/dist/css/bootstrap.min.css',
        filters='cssmin',
        output=CSS_PATH + 'bootstrap.min.css'
    ),
    'chart_js': Bundle(
        NPM_PATH + 'chart.js/dist/Chart.min.js',
        filters='jsmin',
        output=JS_PATH + 'Chart.min.js'
    ),
    'd3_js': Bundle(
        NPM_PATH + 'd3/d3.v2.min.js',
        filters='jsmin',
        output=JS_PATH + 'd3.min.js'
    ),
    'datatables_js': Bundle(
        NPM_PATH + 'datatables.net/js/jquery.dataTables.min.js',
        NPM_PATH + 'datatables.net-responsive/js/dataTables.responsive.min.js',
        NPM_PATH + 'datatables.net-select/js/dataTables.select.min.js',
        NPM_PATH + 'datatables.net-buttons/js/dataTables.buttons.min.js',
        NPM_PATH + 'datatables.net-buttons/js/buttons.html5.min.js',
        NPM_PATH + 'datatables.net-editor/js/dataTables.editor.min.js',
        filters='jsmin',
        output=JS_PATH + 'dataTables.bundle.min.js'
    ),
    'datatables_css': Bundle(
        NPM_PATH + 'datatables.net-responsive-dt/css/responsive.dataTables.min.css',
        NPM_PATH + 'datatables.net-dt/css/jquery.dataTables.min.css',
        NPM_PATH + 'datatables.net-select-dt/css/select.dataTables.min.css',
        NPM_PATH + 'datatables.net-buttons-dt/css/buttons.dataTables.min.css',
        filters='cssmin',
        output=CSS_PATH + 'dataTables.bundle.min.css'
    ),
    'fontawesome_css': Bundle(
        NPM_PATH + '@fortawesome/fontawesome-free/css/all.min.css',
        filters='cssmin',
        output=CSS_PATH + 'fontawesome.all.min.css'
    ),
    'jquery_js': Bundle(
        NPM_PATH + 'jquery/dist/jquery.min.js',
        NPM_PATH + 'jquery-ui-dist/jquery-ui.min.js',
        NPM_PATH + 'jquery.md5/index.js',
        filters='jsmin',
        output=JS_PATH + 'jquery.bundle.min.js'
    ),
    'jquery_css': Bundle(
        NPM_PATH + 'jquery-ui-dist/jquery-ui.min.css',
        NPM_PATH + 'jquery-ui-dist/jquery-ui.structure.css',
        NPM_PATH + 'jquery-ui-dist/jquery-ui.theme.css',
        filters='cssmin',
        output=CSS_PATH + 'jquery-ui.min.css'
    ),
    'morris_js': Bundle(
        NPM_PATH + 'morris.js.so/morris.min.js',
        NPM_PATH + 'raphael/raphael.min.js',
        filters='jsmin',
        output=JS_PATH + 'morris.bundle.min.js'
    ),
    'morris_css': Bundle(
        NPM_PATH + 'morris.js.so/morris.css',
        filters='cssmin',
        output=CSS_PATH + 'morris.css'
    ),
    'pdfmake_js': Bundle(
        NPM_PATH + 'pdfmake/build/pdfmake.min.js',
        NPM_PATH + 'pdfmake/build/vfs_fonts.js',
        filters='jsmin',
        output=JS_PATH + 'pdfmake.bundle.min.js'
    ),
    'yasqe_js': Bundle(
        NPM_PATH + 'yasgui-yasqe/dist/yasqe.bundled.min.js',
        filters='jsmin',
        output=JS_PATH + 'yasqe.bundled.min.js'
    ),
    'yasqe_css': Bundle(
        NPM_PATH + 'yasgui-yasqe/dist/yasqe.min.css',
        filters='cssmin',
        output=CSS_PATH + 'yasqe.min.css'
    )
}

for name, bundle in bundles.items():
    assets.register(name, bundle)
    bundle.build(force=True)

# copy webfonts from Fontawesome
shutil.rmtree('fedsdm/static/webfonts')
shutil.copytree('node_modules/@fortawesome/fontawesome-free/webfonts', 'fedsdm/static/webfonts', dirs_exist_ok=True)

# copy jQuery UI theme
shutil.rmtree('fedsdm/static/css/images')
shutil.copytree('node_modules/jquery-ui-dist/images', 'fedsdm/static/css/images', dirs_exist_ok=True)

# remove webassets cache
shutil.rmtree('fedsdm/static/.webassets-cache')
