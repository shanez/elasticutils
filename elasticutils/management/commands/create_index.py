from optparse import make_option
import inspect
from time import sleep
import sys

from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils import importlib
from django.utils.module_loading import module_has_submodule

from elasticutils import get_es
from elasticutils.contrib.django import Indexable, MappingType, ElasticSearchBuilder


class Command(BaseCommand):
    help = 'Creates index for elastic search'
    base_options = (
        make_option("--clear", action="store_true", dest="clear", default=False,
            help='Clear the old indexes.'
        ),
    )
    option_list = BaseCommand.option_list + base_options

    def handle(self, *args, **options):
        using = settings.ES_URLS

        if options.get('clear', False):
            index_names = [val for (key, val) in settings.ES_INDEXES.items()]
            print
            print "WARNING: This will irreparably DELETE ALL INDEXES (%s) in connection '%s'." % (", ".join(index_names), "', '".join(using),)
            print "Your choices after this are to restore from backups or rebuild via the `rebuild_index` command."

            yes_or_no = raw_input("Are you sure you wish to continue? [y/N] ")
            print

            if not yes_or_no.lower().startswith('y'):
                print "No action taken."
                sys.exit()

            ElasticSearchBuilder.delete_indexes()

        ElasticSearchBuilder.create_indexes()

        print 'Sleeping for 5 seconds to avoid race conditions'
        sleep(5)  # Avoid race conditions on mappings

        print 'Creating mappings...'
        for app in settings.INSTALLED_APPS:
            mod = importlib.import_module(app)

            try:
                search_index_module = importlib.import_module("%s.search" % app)
            except ImportError:
                if module_has_submodule(mod, 'search'):
                    raise

                continue
            for item_name, item in inspect.getmembers(search_index_module, inspect.isclass):
                if item and issubclass(item, (Indexable,)) and issubclass(item, (MappingType,)):
                    ElasticSearchBuilder.create_mapping(item)
