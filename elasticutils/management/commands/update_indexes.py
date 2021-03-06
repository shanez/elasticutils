import inspect
from django.core.management.base import BaseCommand
from django.conf import settings
from django.utils import importlib
from django.utils.module_loading import module_has_submodule

from elasticutils.contrib.django.tasks import index_objects
from elasticutils.contrib.django import Indexable, MappingType


class Command(BaseCommand):

    help = 'Creates index for elastic search'

    def handle(self, *args, **options):
        for app in settings.INSTALLED_APPS:
            mod = importlib.import_module(app)

            try:
                search_index_module = importlib.import_module("%s.mappings" % app)
            except ImportError:
                if module_has_submodule(mod, 'mappings'):
                    raise

                continue
            for item_name, item in inspect.getmembers(search_index_module, inspect.isclass):
                if item and issubclass(item, (Indexable,)) and issubclass(item, (MappingType,)) and item.model and item.mapping_type_name:
                    documents = [a for a in item.get_model().objects.all().values_list("id", flat=True)]
                    index_objects(item, documents)
