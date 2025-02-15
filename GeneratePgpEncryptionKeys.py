import gnupg

#create the gpg object
gpg = gnupg.GPG()
print("version is ",gpg.version)

# Generate keys passphrase and email is necessary
input_data = gpg.gen_key_input(name_email='test@gmail.com', passphrase='test123')

# Generate key
key = gpg.gen_key(input_data)
print(key)

# Export public and private keys so it can be saved later
ascii_armored_public_keys = gpg.export_keys(key.fingerprint)
ascii_armored_private_keys = gpg.export_keys(key.fingerprint, True,passphrase='test123')





# # Write private key to a file
with open('my_private_key.asc', 'w') as f:
    f.writelines(ascii_armored_private_keys)


# # Write public key to a file
with open('my_public_key.asc', 'w') as f:
    f.writelines(ascii_armored_public_keys)
