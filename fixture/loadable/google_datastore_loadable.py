
from fixture.loadable import EnvLoadableFixture

class GoogleDatastoreMedium(EnvLoadableFixture.StorageMediumAdapter):
    
    def clear(self, obj):
        """Delete this object from the DB"""
        obj.delete()
        
    def save(self, row, column_vals):
        """Save this row to the DB"""
        entity = self.medium(
            **dict([(k,v) for k,v in column_vals])
        )
        entity.put()
        return entity
    
class GoogleDatastoreFixture(EnvLoadableFixture):
    Medium = GoogleDatastoreMedium
    
    def commit(self):
        """call transaction.commit() on transaction returned by :meth:`DBLoadableFixture.create_transaction`"""
        pass
    
    def rollback(self):
        """call transaction.rollback() on transaction returned by :meth:`DBLoadableFixture.create_transaction`"""
        pass
        