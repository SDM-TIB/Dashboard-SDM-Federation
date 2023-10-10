import os.path
import shutil

from cssmin import cssmin
from rjsmin import jsmin


class Bundle(object):
    """A bundle is a unit to organize groups of media files, which filters to apply and where to store them.

    This class is inspired by the class ``Bundle`` from the ``webassets`` library.
    However, this class is only for the use within this project, and, hence, does not need to be general.
    This leads to the advantage that an instance of this class does not need an ``Environment`` class for
    translating relative paths to absolute ones. All paths are fixed for the use within this project.

    We are not relying on ``flask_assets`` and ``webassets`` as it seems that they are not actively
    maintained anymore. Additionally, those library do not work with Flask 3.0+.

    """

    STATIC_FOLDER = 'FedSDM/static/'

    def __init__(self, *contents, **options):
        self.contents = contents
        self.output = options.pop('output', None)
        self.filters = options.pop('filters', None)

        if self.output is not None:
            self.output = self.STATIC_FOLDER + self.output

    def build(self):
        """Builds the bundle.

        All files are combined and stored at the specified location.
        If any filters where given, they are applied before storing the combined bundle.

        Other than with the original implementation of this method, the output will always be generated;
        simulating a ``Bundle.build()`` call with ``force=True``.

        Step 1: Read all input files into one string
        Step 2: Apply filters
        Step 3: Write output file

        """
        if self.contents is None or self.output is None:
            return

        content = ''
        for file in self.contents:
            if not os.path.isfile(file):
                continue

            content += open(file, 'r').read()
            if not content.endswith('\n'):
                content += '\n'

        if self.filters == 'cssmin':
            content = cssmin(content)
        elif self.filters == 'rjsmin':
            content = jsmin(content, keep_bang_comments=False)
        else:
            raise NotImplementedError('Filter ' + self.filters + ' not implemented.')

        out_dir = os.path.dirname(self.output)
        os.makedirs(out_dir, exist_ok=True)
        open(self.output, 'w').write(content)


NPM_PATH = 'node_modules/'
JS_PATH = 'js/libs/'
CSS_PATH = 'css/'
WEBFONTS_PATH = 'webfonts/'

bundles = {
    'bootstrap_js': Bundle(
        NPM_PATH + 'bootstrap/dist/js/bootstrap.bundle.min.js',
        filters='rjsmin',
        output=JS_PATH + 'bootstrap.min.js'
    ),
    'bootstrap_css': Bundle(
        NPM_PATH + 'bootstrap/dist/css/bootstrap.min.css',
        filters='cssmin',
        output=CSS_PATH + 'bootstrap.min.css'
    ),
    'chart_js': Bundle(
        NPM_PATH + 'chart.js/dist/chart.umd.js',
        filters='rjsmin',
        output=JS_PATH + 'Chart.min.js'
    ),
    'd3_js': Bundle(
        NPM_PATH + 'd3/d3.v2.min.js',
        filters='rjsmin',
        output=JS_PATH + 'd3.min.js'
    ),
    'datatables_js': Bundle(
        NPM_PATH + 'datatables.net/js/jquery.dataTables.min.js',
        NPM_PATH + 'datatables.net-responsive/js/dataTables.responsive.min.js',
        NPM_PATH + 'datatables.net-select/js/dataTables.select.min.js',
        NPM_PATH + 'datatables.net-buttons/js/dataTables.buttons.min.js',
        NPM_PATH + 'datatables.net-buttons/js/buttons.html5.min.js',
        NPM_PATH + 'jszip/dist/jszip.min.js',
        filters='rjsmin',
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
        NPM_PATH + 'jquery.md5/index.js',
        filters='rjsmin',
        output=JS_PATH + 'jquery.bundle.min.js'
    ),
    'pdfmake_js': Bundle(
        NPM_PATH + 'pdfmake/build/pdfmake.min.js',
        NPM_PATH + 'pdfmake/build/vfs_fonts.js',
        filters='rjsmin',
        output=JS_PATH + 'pdfmake.bundle.min.js'
    ),
    'yasqe_js': Bundle(
        NPM_PATH + 'yasgui-yasqe/dist/yasqe.bundled.min.js',
        filters='rjsmin',
        output=JS_PATH + 'yasqe.bundled.min.js'
    ),
    'yasqe_css': Bundle(
        NPM_PATH + 'yasgui-yasqe/dist/yasqe.min.css',
        filters='cssmin',
        output=CSS_PATH + 'yasqe.min.css'
    )
}

for name, bundle in bundles.items():
    bundle.build()

# copy webfonts from Fontawesome
shutil.rmtree('FedSDM/static/webfonts')
shutil.copytree('node_modules/@fortawesome/fontawesome-free/webfonts', 'FedSDM/static/webfonts', dirs_exist_ok=True)
