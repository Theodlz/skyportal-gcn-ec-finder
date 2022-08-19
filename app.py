import os
import time
from datetime import datetime, timedelta
import arrow

sp = __import__('skyportal-dumps')
from utils.log import make_log

log = make_log('analyse_sources_in_gcn')

def analyse_sources_in_gcn(analysis_service: dict = None, skyportal_url: str = None, skyportal_token: str = None):
    now = datetime.utcnow()
    then = now - timedelta(days=7)

    status, gcn_events = sp.get_all_gcnevents(startDate=then, endDate=now, url=skyportal_url, token=skyportal_token)

    if status == 200:
        for gcnevent in gcn_events:
            localizationDateobs = gcnevent['dateobs']
            most_recent_localization = gcnevent['localizations'][0]
            for localization in gcnevent['localizations'][1:]:
                if arrow.get(localization['created_at']) > arrow.get(most_recent_localization['created_at']):
                    most_recent_localization = localization
            localizationName = most_recent_localization['localization_name']
            # get the sources for this gcnevent
            endDate = (arrow.get(localizationDateobs) + timedelta(days=7)).format('YYYY-MM-DDTHH:mm:ss')
            status, sources = sp.get_all_sources_and_phot(localizationDateobs=localizationDateobs, localizationName=localizationName, startDate=localizationDateobs, endDate=endDate, localizationCumprob=0.50, numberDetections=2, numPerPage=100, url=skyportal_url, token=skyportal_token, whitelisted=False)
            if status == 200:
                if len(sources) > 0:
                    for source in sources:
                        status, analysis = sp.get_analysis_from_source(source_id=source['id'], url=skyportal_url, token=skyportal_token)
                        if status == 200:
                            already_analyzed = False
                            if len(analysis) > 0:
                                analysis_with_nmma = [a for a in analysis if a['analysis_service_id'] == analysis_service['id']]
                                if len(analysis_with_nmma) > 0:
                                    if any(a['status'] == 'completed' for a in analysis_with_nmma):
                                        already_analyzed = True
                                    elif any(a['status'] != 'failed' and arrow.get(a['created_at']) > (now - timedelta.microseconds(60000000)) for a in analysis_with_nmma):
                                        already_analyzed = True
                            if not already_analyzed:
                                status = sp.start_nmma_analysis(source_id=source['id'], analysis_service_id=analysis_service['id'], url=skyportal_url, token=skyportal_token)
                                if status == 200:
                                    log(f'started analysis on {source["id"]} contained in {localizationDateobs} - {localizationName}')

def main():
    if 'SKYPORTAL_URL' in os.environ:
        skyportal_url = os.environ['SKYPORTAL_URL']
    else:
        log('SKYPORTAL_URL not found in environment')
        return
    if 'SKYPORTAL_TOKEN' in os.environ:
        skyportal_token = os.environ['SKYPORTAL_TOKEN']
    else:
        log('SKYPORTAL_TOKEN not found in environment')
        return
    status, analysis_service = sp.get_analysis_service(name='NMMA_Analysis', url=skyportal_url, token=skyportal_token)
    if status != 200 or analysis_service == {}:
        log('Error: NMMA analysis service not found')
        return
    start_time = time.perf_counter()
    analyse_sources_in_gcn(analysis_service=analysis_service, skyportal_url=skyportal_url, skyportal_token=skyportal_token)
    end_time = time.perf_counter()
    log('Time elapsed: {:.2f} seconds'.format(end_time - start_time))
    time.sleep(60)

if __name__ == '__main__':
    main()


