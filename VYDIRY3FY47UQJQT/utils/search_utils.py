import streamlit as st
from typing import List, Dict, Any
from utils.snowflake_utils import session, root


def query_cortex_search_service(query: str, service: str, limit: int, metadata: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Execute a search against a specified Cortex Search service and format results.
    """
    
    if not service:
        st.warning("Please select a search service.")
        return []

    idx = next((i for i, m in enumerate(metadata) if m["name"] == service), None)
    if idx is None or not metadata[idx]["search_column"]:
        st.error(f"Invalid service metadata for '{service}'.")
        return []

    search_col = metadata[idx]["search_column"]
    svc = (
        root.databases[session.get_current_database()]
            .schemas[session.get_current_schema()]
            .cortex_search_services[service]
    )
    try:
        resp = svc.search(
            query=query,
            columns=[search_col, 'FILE_NAME', 'SELLER_NAME_VALUE',
                     'CLIENT_NAME_VALUE', 'TOTAL_GROSS_WORTH_VALUE', 'ISSUE_DATE_VALUE'],
            limit=limit,
            group_by_field="FILE_NAME"
        )
    except Exception as e:
        st.error(f"Search failed: {e}")
        return []

    # Format and return results list
    return [
        {
            "file": rec.get('FILE_NAME', '<unknown>'),
            "chunk": rec.get(search_col, ''),
            "seller": rec.get('SELLER_NAME_VALUE'),
            "client": rec.get('CLIENT_NAME_VALUE'),
            "total_gross": rec.get('TOTAL_GROSS_WORTH_VALUE'),
            "issue_date": rec.get('ISSUE_DATE_VALUE'),
        }
        for rec in resp.results
    ]