from mcp.server.fastmcp import FastMCP
import os
from datetime import datetime, timedelta

from google.cloud import bigquery
client = bigquery.Client()

mcp = FastMCP("GDELT Sentiment Pipeline", json_response=True)

@mcp.tool()
def related_themes(topic: str, start_date: int, end_date: int) -> str:
    """
    Query the GDELT database for related themes to a given topic.

    Args:
        topic (str): The topic to search for.
        start_date (str): The start date to search for in the format YYYYMMDDHHMMSS.
        end_date (str): The end date to search for in the format YYYYMMDDHHMMSS.

    Returns:
        str: A list of related themes to the given topic.
    """
    query = f"""SELECT theme, COUNT(*) as count
                FROM (
                    SELECT REGEXP_REPLACE(theme, r',.*', '') as theme
                    FROM `gdelt-bq.gdeltv2.gkg`,
                    UNIQUE(UNNEST(SPLIT(V2Themes,';'))) as theme
                    WHERE DATE>{start_date} and DATE < {end_date} and LOWER(V2Themes) like CONCAT('%', LOWER('{topic}'), '%')
                )
                GROUP BY theme
                ORDER BY 2 DESC
                LIMIT 5"""
    query_job = client.query(query)
    rows = query_job.result()
    results = list(rows)
    if not results:
        return "No themes found for the specified topic and date range."
    return "\n".join([f"{row.theme}: {row.count}" for row in results])

@mcp.tool()
def get_topic_sentiment(topic: str, days: int) -> str:
    """
    Get the sentiment trend for a given topic over a given number of days.

    Args:
        topic (str): The topic to search for.
        days (int): The number of days to search for.

    Returns:
        str: A list of sentiment trend for the given topic and number of days.
    """
    days_ago_date = datetime.now() - timedelta(days=days)
    start_date = int(days_ago_date.strftime('%Y%m%d') + '000000')
    
    query = f"""SELECT 
                    SUBSTR(CAST(DATE AS STRING), 1, 8) as day, 
                    AVG(CAST(SPLIT(V2Tone, ',')[OFFSET(0)] AS FLOAT64)) as avg_sentiment, 
                    COUNT(*) as volume
                FROM `gdelt-bq.gdeltv2.gkg`
                WHERE LOWER(V2Themes) LIKE CONCAT('%', LOWER('{topic}'), '%')
                AND DATE >= {start_date}
                AND V2Tone IS NOT NULL
                GROUP BY day
                ORDER BY day DESC
                LIMIT 100"""
    query_job = client.query(query)
    rows = query_job.result()
    results = list(rows)
    if not results:
        return "No sentiment trend found for the specified topic and number of days."
    return "\n".join([f"{row.day}: {row.avg_sentiment}: {row.volume}" for row in results])

@mcp.tool()
def get_person_sentiment(person: str, days: int) -> str:
    """
    Get the sentiment trend for a given person over a given number of days.

    Args:
        person (str): The person to search for.
        days (int): The number of days to search for.

    Returns:
        str: A list of sentiment trend for the given person and number of days.
    """
    days_ago_date = datetime.now() - timedelta(days=days)
    start_date = int(days_ago_date.strftime('%Y%m%d') + '000000')
    
    query = f"""SELECT 
                    SUBSTR(CAST(DATE AS STRING), 1, 8) as day, 
                    AVG(CAST(SPLIT(V2Tone, ',')[OFFSET(0)] AS FLOAT64)) as avg_sentiment, 
                    COUNT(*) as volume
                FROM `gdelt-bq.gdeltv2.gkg`
                WHERE LOWER(V2Persons) LIKE CONCAT('%', LOWER('{person}'), '%')
                AND DATE >= {start_date}
                AND V2Tone IS NOT NULL
                GROUP BY day
                ORDER BY day DESC
                LIMIT 100"""
    query_job = client.query(query)
    rows = query_job.result()
    results = list(rows)
    if not results:
        return "No sentiment trend found for the specified person and number of days."
    return "\n".join([f"{row.day}: {row.avg_sentiment}: {row.volume}" for row in results])
    

if __name__ == "__main__":
    mcp.run()
