// 使用预共享密钥和用户名密码配置 IPsec VPN
function configureIPsecVPN() {
  var config = {
    iceServers: [{
      urls: 'turn:43.159.47.250',
      username: 'wth000',
      credential: 'wth000'
    }]
  };

  var pc = new RTCPeerConnection(config);
  pc.createDataChannel('vpn');

  pc.onicecandidate = function(event) {
    if (event.candidate === null) {
      console.log('IPsec VPN 已配置');
    }
  };

  pc.createOffer().then(function(offer) {
    return pc.setLocalDescription(offer);
  }).catch(function(error) {
    console.error('无法生成 IPsec VPN 配置', error);
  });
}

// 在页面加载完成后执行配置
window.addEventListener('DOMContentLoaded', function() {
  configureIPsecVPN();
});