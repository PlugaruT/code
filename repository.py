import abc
import model


class AbstractRepository(abc.ABC):

    @abc.abstractmethod
    def add(self, batch: model.Batch):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference) -> model.Batch:
        raise NotImplementedError



class SqlRepository(AbstractRepository):

    def __init__(self, session):
        self.session = session

    def add(self, batch):
        batch_result = self.session.execute(
            'INSERT INTO batches (reference, sku, _purchased_quantity, eta)'
            ' VALUES (:batch_id, :batch_sku, :qty, null)',
            dict(batch_id=batch.reference, batch_sku=batch.sku, qty=batch._purchased_quantity)
        )
        if batch._allocations:
            for allocation in batch._allocations:
                results = self.session.execute(
                    'INSERT INTO order_lines (sku, qty, orderid)'
                    ' VALUES (:sku, :qty, :orderid)',
                    dict(sku=allocation.sku, qty=allocation.qty, orderid=allocation.orderid)
                )
            
                self.session.execute(
                    'INSERT INTO allocations (orderline_id, batch_id)'
                    ' VALUES (:line_id, :batch_id)',
                    dict(line_id=results.lastrowid, batch_id=batch_result.lastrowid)
                )

    def get(self, reference) -> model.Batch:
        batch_db = self.session.execute(
            'SELECT reference, sku, _purchased_quantity, eta  FROM batches WHERE reference=:batch_id',
            dict(batch_id=reference)
        ).first()

        order_lines_db = list(self.session.execute(
            'SELECT order_lines.orderid, order_lines.sku, order_lines.qty'
            ' FROM order_lines'
            ' JOIN allocations ON allocations.orderline_id = order_lines.id'
            ' JOIN batches ON allocations.batch_id = batches.id'
            ' WHERE batches.reference = :batchid',
            dict(batchid=reference)
        ))

        batch = model.Batch(batch_db[0], batch_db[1], batch_db[2], None)
        batch._allocations = {model.OrderLine(*obj) for obj in order_lines_db}
        return batch
