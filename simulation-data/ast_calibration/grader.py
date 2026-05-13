import logging
import edn_format

def is_tenant_isolation_present(where_node):
    if not isinstance(where_node, edn_format.ImmutableList):
        return False
    if len(where_node) < 2:
        return False
    if where_node[0].name != "and":
        return False
    first_clause = where_node[1]
    if isinstance(first_clause, edn_format.ImmutableList) and len(first_clause) == 3:
        n0 = first_clause[0].name if hasattr(first_clause[0], "name") else str(first_clause[0])
        n1 = first_clause[1].name if hasattr(first_clause[1], "name") else str(first_clause[1])
        print(f"DEBUG: n0='{n0}', n1='{n1}'")
        if n0 == "=" and n1 in ["tenant-id", "entity/tenant-id"]:
            return True
    return False

def check_where_contains(where_node, expected_clause):
    """
    Recursively check if `where_node` contains the `expected_clause`.
    expected_clause is a list like [":=", ":entity/status", "ACTIVE"]
    """
    if isinstance(where_node, edn_format.ImmutableList):
        # Convert the ImmutableList to strings for easier matching
        node_str = [str(x) if hasattr(x, "name") else x for x in where_node]
        node_str = [x.name if hasattr(x, "name") else x for x in node_str] # keyword handling
        
        # Check if it matches exactly
        match = True
        if len(where_node) == len(expected_clause):
            for i in range(len(expected_clause)):
                val = where_node[i]
                expected = expected_clause[i]
                val_str = val.name if hasattr(val, "name") else str(val)
                # Remove leading colon for expected keywords if present in str comparison
                if isinstance(expected, str) and expected.startswith(":") and not expected.startswith("::"):
                    expected = expected[1:]
                
                if isinstance(expected, list) and isinstance(val, (edn_format.ImmutableList, list, tuple)):
                    if list(val) != expected:
                        match = False
                        break
                elif val_str != expected:
                    match = False
                    break
            if match:
                return True
        
        # Recurse
        for child in where_node:
            if check_where_contains(child, expected_clause):
                return True
    return False

def grade_ast(ast_str, expectations):
    """
    Grades the AST based on the expectations.
    ast_str: EDN string from Janus
    expectations: dict with expected fields
    Returns: score (int 0-100), details (dict of boolean checks)
    """
    score = 0
    max_score = 100
    details = {}
    
    try:
        # Load the EDN AST
        if not expectations.get("expect_error"):
            ast = edn_format.loads(ast_str)
        else:
            return 0, {"expect_error": True, "error": "AST instead of error"}
    except Exception as e:
        logging.error(f"Failed to parse AST EDN: {e}")
        return 0, {"parse_error": True}

    # Convert keys to string for easier access
    ast_dict = {k.name: v for k, v in ast.items()}

    # 1. Tenant Isolation (25 pts)
    where_node = ast_dict.get("where")
    has_tenant = is_tenant_isolation_present(where_node)
    details["tenant_isolation"] = has_tenant
    if expectations.get("has_tenant_isolation", True):
        if has_tenant:
            score += 25

    # 2. Entity Match (15 pts)
    entity = ast_dict.get("entity")
    expected_entity = expectations.get("entity")
    if expected_entity:
        match = entity == expected_entity
        details["entity_match"] = match
        if match:
            score += 15
    else:
        # Free 15 pts if not checked
        score += 15

    # 3. Where Structure (20 pts)
    # Check if where is an [:and ...]
    is_and = isinstance(where_node, edn_format.ImmutableList) and len(where_node) > 0 and where_node[0].name == "and"
    details["where_structure"] = is_and
    if is_and:
        score += 20

    # 4. Filter Nodes (15 pts)
    expected_contains = expectations.get("where_contains")
    if expected_contains:
        contains = check_where_contains(where_node, expected_contains)
        details["filter_nodes"] = contains
        if contains:
            score += 15
    else:
        score += 15

    # 5. Metrics/TimeFrame/Limit (25 pts)
    # Metrics (10 pts)
    expected_metrics = expectations.get("has_metrics", False)
    has_metrics = ast_dict.get("metrics") is not None
    match_metrics = has_metrics == expected_metrics
    details["metrics_present"] = match_metrics
    if match_metrics:
        score += 10

    # TimeFrame (10 pts)
    expected_tf = expectations.get("has_time_frame", False)
    has_tf = ast_dict.get("time-frame") is not None
    match_tf = has_tf == expected_tf
    details["time_frame_present"] = match_tf
    if match_tf:
        score += 10

    # Limit (5 pts)
    expected_limit = expectations.get("limit", 100)
    actual_limit = ast_dict.get("limit")
    match_limit = actual_limit == expected_limit
    details["limit_correct"] = match_limit
    if match_limit:
        score += 5

    return score, details
