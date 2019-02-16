from gm.gm import GazetteMachine


def identify_and_archive(event, context):
    gm = GazetteMachine()
    return gm.identify_and_archive(event)


if __name__ == '__main__':
    import ipdb; ipdb.set_trace()
    print(GazetteMachine().identify_and_archive({
        'fname': '3564.pdf',
        's3_location': 's3/3564.pdf',
        'jurisdiction': 'na',
    }))
