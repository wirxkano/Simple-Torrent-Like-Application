import libtorrent as lt
import os

def make_torrent(file_path, torrent_file_path):
    #if not os.path.isfile(file_path):
     #   raise FileNotFoundError(f"The file at {file_path} does not exist.")

    fs = lt.file_storage()
    lt.add_files(fs, file_path)


    t = lt.create_torrent(fs)

    t.add_tracker("127.0.0.1:12345")

    t.set_creator("My Python Torrent Creator")
    t.set_comment("This is a sample torrent file created for testing.")


    lt.set_piece_hashes(t, os.path.dirname(file_path))


    with open(torrent_file_path, "wb") as f:
        f.write(lt.bencode(t.generate()))

    # print(f"Torrent file created at: {os.path.abspath(torrent_file_path)}")
    return torrent_file_path

# make_torrent('/home/wirxkano/PycharmProjects/CN_asssignment1/AP_homework', 'ap_homework2.torrent')