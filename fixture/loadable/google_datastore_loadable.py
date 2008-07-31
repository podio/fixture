
from fixture.loadable import EnvLoadableFixture

class EntityMedium(EnvLoadableFixture.StorageMediumAdapter):
    """
    Adapts google.appengine.api.datastore.Entity objects and any 
    other object that is an instance of Entity
    """
    def clear(self, obj):
        """Delete this entity from the Datastore"""
        obj.delete()
        
    def save(self, row, column_vals):
        """Save this entity to the DB"""
        entity = self.medium(
            **dict([(k,v) for k,v in column_vals])
        )
        entity.put()
        return entity
    
class GoogleDatastoreFixture(EnvLoadableFixture):
    """
    A fixture that knows how to load DataSet objects into Google Datastore `Entity`_ objects.
    
    .. _Entity: http://code.google.com/appengine/docs/datastore/entitiesandmodels.html
    
    Keyword Arguments:
    
    ``style``
        A :class:`Style <fixture.style.Style>` object to translate names with
    
    ``env``
        A dict or module that contains Entity clasess.  This will be searched when 
        :class:`Style <fixture.style.Style>` translates DataSet names into
        storage media.  See :meth:`EnvLoadableFixture.attach_storage_medium <fixture.loadable.loadable.EnvLoadableFixture.attach_storage_medium>` for details on 
        how ``env`` works.
    
    ``dataclass``
        :class:`SuperSet <fixture.dataset.SuperSet>` class to represent loaded data with
    
    ``medium``
        A custom :class:`StorageMediumAdapter <fixture.loadable.loadable.StorageMediumAdapter>` 
        class to instantiate when storing a DataSet.
        By default, an Entity adapter will be used so you should only set a custom medium 
        if you know what you doing.
    """
    Medium = EntityMedium
    
    def commit(self):
        pass
    
    def rollback(self):
        pass
        