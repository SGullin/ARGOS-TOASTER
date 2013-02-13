def flash(request):
  if not ('flash' in request.session):
    return {}
  tmp_flash = request.session['flash']
  del request.session['flash']
  message = tmp_flash['message']
  flash_type    = tmp_flash['type']
  return { 'flash' : { 'message': message, 'type': flash_type } }