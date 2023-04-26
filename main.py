import bz2
import os
import requests
import time

import libtorrent
from absl import app
from absl import flags
from absl import logging

# Flags
FLAGS = flags.FLAGS
flags.DEFINE_string('torrent_link', None, 'URL for multistream torrent file.')
flags.DEFINE_string(
    'output_dir', '/tmp/wikidb/',
    'Location to store Wikipedia multistream file (requires approximately 20 GB).'
)
flags.DEFINE_integer('port', 6881, '.', lower_bound=0)
flags.mark_flag_as_required('torrent_link')


def download_torrent(torrent_link: str, output_dir: str) -> str:
    torrent_session = libtorrent.session({
        'listen_interfaces':
        f'0.0.0.0:{FLAGS.port}',
    })

    # Fetch multistream torrent file
    wiki_torrent_response = requests.get(torrent_link, stream=True)
    wiki_torrent_response.raw.decode_content = True
    wiki_torrent_params = libtorrent.load_torrent_buffer(
        wiki_torrent_response.content)

    # Download torrent
    wiki_torrent_handle = torrent_session.add_torrent({
        'ti':
        wiki_torrent_params.ti,
        'save_path':
        output_dir,
    })
    wiki_torrent_handle.resume()
    wiki_torrent_status = wiki_torrent_handle.status()
    logging.info('Starting download of %s to %s...', wiki_torrent_status.name,
                 wiki_torrent_status.save_path)

    while (not wiki_torrent_status.is_finished):
        wiki_torrent_status = wiki_torrent_handle.status()
        logging.info(
            '%s: %s, %d%% complete... (%d kbps down / %d kbps up) [%d peers]',
            wiki_torrent_status.name,
            wiki_torrent_status.state,
            wiki_torrent_status.progress * 100,
            wiki_torrent_status.download_rate / 1000,
            wiki_torrent_status.upload_rate / 1000,
            wiki_torrent_status.num_peers,
        )

        for alert in torrent_session.pop_alerts():
            if alert.category(
            ) & libtorrent.alert.category_t.error_notification:
                logging.error(alert)

        time.sleep(5)

    # Find and return path for multistream archive
    wiki_multistream_filepath = ''
    for file_entry in os.scandir(FLAGS.output_dir):
        if file_entry.name == wiki_torrent_status.name:
            wiki_multistream_filepath = file_entry.path

    return wiki_multistream_filepath


def main(argv):
    logging.info('Starting WikiDB ETL pipeline...')
    wiki_multistream_filepath = download_torrent(FLAGS.torrent_link,
                                                 FLAGS.output_dir)
    with bz2.open(wiki_multistream_filepath) as wiki_multistream_file:
        for article in wiki_multistream_file:
            print(article)


if __name__ == '__main__':
    app.run(main)
