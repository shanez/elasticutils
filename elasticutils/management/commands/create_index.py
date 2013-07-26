import inspect
from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils import importlib
from django.utils.module_loading import module_has_submodule

from elasticutils import get_es
from elasticutils.contrib.django import Indexable, MappingType


class Command(BaseCommand):

    help = 'Creates index for elastic search'

    def handle(self, *args, **options):
        es = get_es(urls=settings.ES_URLS)

        es_settings = {}
        es_settings.update(settings.ES_SETTINGS)

        mappings = {}
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
                    mappings.update({item.get_mapping_type_name(): item.get_mapping()})

            es_settings.update({"mappings": mappings})

        es.create_index(settings.ES_INDEXES.get('default'), es_settings)
