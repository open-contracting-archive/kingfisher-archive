import os.path


def path(filename):
    return os.path.join('tests', 'fixtures', filename)


def create_crawl_directory(tmpdir, data, log, source_id='scotland'):
    data_directory = tmpdir.mkdir('data')
    spider_directory = data_directory.mkdir(source_id)

    if data is not None:
        crawl_directory = spider_directory.mkdir('20200902_052458')
        for i, name in enumerate(data):
            file = crawl_directory.join(f'{i}.json')
            with open(path(name)) as f:
                file.write(f.read())

    logs_directory = tmpdir.mkdir('logs')
    project_directory = logs_directory.mkdir('kingfisher')
    spider_directory = project_directory.mkdir('scotland')

    if log:
        file = spider_directory.join('307e8331edc801c691e21690db130256.log')
        with open(path(log)) as f:
            file.write(f.read())
