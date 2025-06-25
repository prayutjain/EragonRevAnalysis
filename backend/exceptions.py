# exceptions.py
class QueryEngineError(Exception):
    """Base exception for query engine"""
    pass

class NoResultError(QueryEngineError):
    """Raised when no results found for a query"""
    pass

class BadQuestionError(QueryEngineError):
    """Raised when question cannot be parsed or understood"""
    pass

class DataSourceError(QueryEngineError):
    """Raised when a data source is unavailable"""
    pass

class PlanningError(QueryEngineError):
    """Raised when query planning fails"""
    pass