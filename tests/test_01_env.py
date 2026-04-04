"""
Part 1 — Dev Environment Smoke Test

Verifies that every service dependency is reachable and correctly configured.
Run: python tests/test_01_env.py
"""

import os
import sys

# Ensure we can import from the project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

PASS = "✓"
FAIL = "✗"
results = []


def check_env_vars():
    required = ["NEO4J_URI", "NEO4J_PASSWORD", "ANTHROPIC_API_KEY", "TAVILY_API_KEY"]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        results.append((FAIL, f"Missing environment variables: {', '.join(missing)}"))
        return False
    results.append((PASS, "All environment variables loaded"))
    return True


def check_neo4j():
    from neo4j import GraphDatabase

    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD")

    try:
        driver = GraphDatabase.driver(uri, auth=(user, password))
        with driver.session() as session:
            result = session.run("RETURN 1 AS n")
            record = result.single()
            assert record["n"] == 1

        # Get Neo4j version (fetch first record only to avoid multi-record warning)
        with driver.session() as session:
            version_result = session.run("CALL dbms.components() YIELD name, versions RETURN name, versions LIMIT 1")
            version_record = version_result.single()
            version = version_record["versions"][0] if version_record else "unknown"

        driver.close()
        results.append((PASS, f"Neo4j connected (version {version})"))
        return True
    except Exception as e:
        results.append((FAIL, f"Neo4j connection failed: {e}"))
        return False


def check_claude():
    import anthropic

    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    try:
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=64,
            messages=[
                {
                    "role": "user",
                    "content": "Reply with exactly this string and nothing else: Hello from Systematic Problem Scouting",
                }
            ],
        )
        response_text = message.content[0].text.strip()
        results.append((PASS, f"Claude API: claude-sonnet-4-6 responded — '{response_text}'"))
        return True
    except Exception as e:
        results.append((FAIL, f"Claude API failed: {e}"))
        return False


def check_tavily():
    from tavily import TavilyClient

    try:
        client = TavilyClient(api_key=os.getenv("TAVILY_API_KEY"))
        response = client.search("Ipsos market research", max_results=3)
        count = len(response.get("results", []))
        results.append((PASS, f"Tavily Search API: returned {count} results for 'Ipsos market research'"))
        return True
    except Exception as e:
        results.append((FAIL, f"Tavily Search API failed: {e}"))
        return False


def main():
    print("\n=== Systematic Problem Scouting — Environment Smoke Test ===\n")

    checks = [
        ("Environment variables", check_env_vars),
        ("Neo4j", check_neo4j),
        ("Claude API", check_claude),
        ("Tavily Search API", check_tavily),
    ]

    all_passed = True
    for name, check_fn in checks:
        try:
            passed = check_fn()
        except Exception as e:
            results.append((FAIL, f"{name} raised unexpected error: {e}"))
            passed = False
        if not passed:
            all_passed = False

    print("Results:")
    for icon, message in results:
        print(f"  {icon} {message}")

    print()
    if all_passed:
        print("All checks passed. Ready to proceed to Part 2.")
    else:
        print("Some checks failed. Fix the issues above before proceeding.")
        sys.exit(1)


if __name__ == "__main__":
    main()
