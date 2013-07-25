import inspect
from optparse import make_option

from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils import importlib
from django.utils.module_loading import module_has_submodule

from elasticutils.contrib.django.tasks import index_objects
from elasticutils.contrib.django import Indexable, MappingType


class Command(BaseCommand):

    help = 'Creates index for elastic search'
    base_options = (
        make_option("-d", "--doctype", action="append", dest="doctype",
            default=[],
            help='Names of the doctypes to update'
        ),
    )

    def handle(self, *args, **options):
        update_all = True
        if options.get('indexes'):
            update_all = False

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
                    if update_all or item.get_mapping_type_name() in options.get('indexes'):
                        documents = [a for a in item.get_model().objects.all().values_list("id", flat=True)]
                        index_objects(item, documents)

