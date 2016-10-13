/**
 * RuzeeBorders 0.12alpha!!!!!!!!!!!!!!
 * (c) 2006 Steffen Rusitschka <steffen@rusitschka.de>
 *
 * RuzeeBorders is freely distributable under the terms of an MIT-style license.
 * For details, see http://www.ruzee.com/
 */


var RUZEE=window.RUZEE||{};
RUZEE.userAgent=navigator.userAgent.toLowerCase();
RUZEE.isIE=typeof window.RUZEE.isIE != 'undefined'
  ?window.RUZEE.isIE
  :RUZEE.userAgent.indexOf('msie')>=0
    && RUZEE.userAgent.indexOf('opera')==-1;
RUZEE.isStrict=typeof window.RUZEE.isStrict != 'undefined'
  ?window.RUZEE.isStrict
  :(document.compatMode?document.compatMode!='BackCompat':RUZEE.userAgent.indexOf('safari')==-1?false:true);

RUZEE.Borders={

  /**
   * Set to false to not draw the borders automatically on
   * domload when RUZEE.Events are available.
   */
  autoRender:true,

  /** Add mapping rules to be executed on render(). */
  add:function(mappings){
    for(rule in mappings){
      var rules=rule.split(',');
      for(var i=0; i<rules.length; ++i){
        var r=rules[i].replace(/^\s+|\s+$/,'');
        var ms=RUZEE.Borders.mappings[r]||{};
        for (m in mappings[rule]) ms[m]=mappings[rule][m];
        RUZEE.Borders.mappings[r]=ms;
      }
    }
  },

  /**
   * Render all added mapping rules into the DOM 
   * If RUZEE.Events is not available, this method MUST be called in the 
   * window.onload method (or with a similar technique)!
   */
  render:function(){
    var jobs=[];
    for(rule in RUZEE.Borders.mappings){
      jobs.push({
        e:RUZEE.Borders.cssQuery(rule),
        b:new RUZEE.Borders.Border(RUZEE.Borders.mappings[rule])
      });
    }
    RUZEE.Borders.mappings={};
    for (var i=0; i<jobs.length; ++i){
      jobs[i].b.calc(jobs[i].e);
    }
    RUZEE.Borders.renderCalcs();
  },

  /** The Border class constructor */
  Border:function(d){
    var rad=d.cornerRadius||8;
    this.shadowShift=0;
    this.setEdges(d.edges||'lrtb');
    this.height=d.height||0;
    var b=null;
    switch(d.borderType){
    case 'simple':
      this.cornerRadius=this.shadowRadius=this.shadowPadding=rad;
      this.coShadowS='000';
      break;
    case 'shadow':
      var sw=d.shadowWidth||8;
      this.cornerRadius=rad;
      this.shadowRadius=rad+sw*2;
      this.shadowPadding=rad+sw;
      this.shadowShift=Math.round(sw/2);
      this.coShadowS=d.shadowColor||'000';
      break;
    case 'fade':
      this.cornerRadius=this.shadowPadding=1;
      this.shadowRadius=rad;
      this.coShadowS='.fade';
      break;
    case 'glow':
      this.cornerRadius=this.shadowPadding=rad;
      this.shadowRadius=rad+(d.glowWidth||rad);
      this.coShadowS=d.glowColor||'fff';
      break;
    default:
      alert('Unknown borderType: '+d.borderType);
    }
  },

  // ---- internal fields and methods ----

  /** the mappings: 'CSS rule' -> Border */
  mappings:{},

  /** The corner cache */
  cache:{},

  /** The completed calulations to render */
  calcs:[],

  /** if Dean Edward's cssQuery is available, use it */
  cssQuery:function(s){
    var c=s.charAt(0);
    if(c=='#'&&!(/\s/.test(s))) return [ document.getElementById(s.substr(1)) ];
    if(window.cssQuery) return window.cssQuery(s);

    alert("Don't know what to do with '"+s+"' Did you forget to include cssquery?");
    return [];
  },

  /** Add a completed calculation */
  addCalc:function(calc){
    RUZEE.Borders.calcs.push(calc);
  },

  renderCalcs:function(){
    for(var i=0; i<RUZEE.Borders.calcs.length; ++i){
      RUZEE.Borders.calcs[i]();
    }
    RUZEE.Borders.calcs=[];
  }
};

/** The Border class */
RUZEE.Borders.Border.prototype={

  /** Set the background image for element e to position x,y */
  setBgImg:function(e,x,y){
    if(!this.imgBgInURL) return;
    e.style.backgroundImage=this.imgBgInURL;
    x=-x;y=-y;
    e.style.backgroundPosition=x+'px '+y+'px';
    if(this.imgBgInRepeat) e.style.backgroundRepeat=this.imgBgInRepeat;
  },

  /** Create a DIV with width w, height h, background color bg, overflow o */
  crDiv:function(w,h,bg,o){
    var d=RUZEE.isXHTML
      ?document.createElementNS('http://www.w3.org/1999/xhtml','div')
      :document.createElement('div');
    d.style.padding=d.style.margin='0px';
    d.style.display='block';
    d.style.border='none';
    d.style.width=w?w:'auto';
    if(h) { d.style.height=h; d.style.fontSize=h; }
    if(!bg) bg='transparent';
    d.style.background=bg;
    if(o) d.style.overflow=o;
    return d;
  },

  /** Create wrapper DIV around element c */
  addLR:function(c,co,w,h,bgx,bgy){
    var e=this.crDiv(null,h,co);
    if(typeof bgx!='undefined') this.setBgImg(e,bgx,bgy);
    if(!w) w='1px';
    c.style.margin='0px '+(this.isR?w:'0px')+' 0px '+(this.isL?w:'0px');
    e.appendChild(c);
    return e;
  },

  /** Create the top (top==true) or bottom (top==false) of the border */
  crTB:function(top){
    var ca=RUZEE.Borders.cache[this.cacheID+'.'+top];
    if(ca){
      if(top){
        this.psT=ca.ps;
        this.inSh=ca.inSh;
      }else{
        this.psB=ca.ps;
      }
      return ca.el.cloneNode(true);
    }
    var sh=top?-this.shadowShift:this.shadowShift;
    var cxc=this.shadowPadding-this.cornerRadius-1;
    var cxb=cxc;
    var cxe=cxc+this.cornerRadius;
    var exb=0;
    var exe=cxc-1;
    var syc=this.cornerRadius-this.shadowPadding+sh+1;
    var yb,ye;
    if(top){
      if(!this.isT){
        this.psT=0;
        return;
      }
      yb=syc+this.shadowRadius-1;
      ye=syc-1;
      yi=-1;
      this.inSh=syc-1;
      this.psT=yb-ye;
    }else{
      if(!this.isB) {
        this.psB=0;
        return;
      }
      yb=syc<0?syc:0;
      ye=syc+this.shadowRadius;
      yi=1;
      this.psB=ye-yb;
    }
    var cwb=this.wBorder;
    if(cwb==0) cwb=1;

    var e=this.crDiv(null, Math.abs(yb-ye)+'px',null,'hidden');
    for(var y=yb; y!=ye; y+=yi){
      var co;
      if(y<=this.cornerRadius-cwb){
        co=this.coBgIn;
      }else if(y<=this.cornerRadius){
        co=this.coBorder;
      }else if(y-syc<0){
        co=this.coShadow;
      }else{
        co=rzBlend(this.coShadow,this.coBgOut,(y-syc)/this.shadowRadius);
      }
      var line=this.crDiv(null,'1px',rzC2S(co),'hidden');
      var fstLine=line;
      var xbg=null;
      for(var x=0; x<this.shadowRadius; ++x){
        var isIn=false, setBgImg=false;
        var sd, out=0;
        if(y<syc){
          sd=x;
        }else{
          sd=Math.sqrt(Math.sqr(x)+Math.sqr(y-syc));
        }
        if(this.shadowRadius>this.cornerRadius && sd<=this.shadowRadius){
          co=rzBlend(this.coShadow, this.coBgOut, sd/this.shadowRadius);
        }else{
          co=this.coBgOut;
          out++;
        }
        if(y<=this.cornerRadius){
          if(x>=exb && x<=exe){
            if(y>this.cornerRadius-cwb){
              co=this.coBorder;
            }else{
              isIn=true;
            }
          }else if(x>=cxb && x<=cxe){
            var cd=Math.sqrt(Math.sqr(x-cxc)+Math.sqr(y))-this.cornerRadius;
            if(y<0){
              if(x-cxc>this.cornerRadius-this.wBorder){
                co=this.coBorder;
              }else{ 
                isIn=true;
              }
            }else if(cd<-cwb){
              isIn=true;
            }else if(cd<-cwb+1){
              // first on border! do bgimg
              if(top&&this.imgBgInURL){
                setBgImg=true;
              }else
                co=rzBlend(this.coBgIn,this.coBorder,cd+cwb);
            }else if(cd<0){
              co=this.coBorder;
            }else if(cd<=1){
              co=rzBlend(this.coBorder,co,cd);
            }else{
              out++;
            }
          }
        }else{
          out++;
        }
        if(!isIn&&line==fstLine&&y<=this.cornerRadius-cwb&&top){
          this.setBgImg(fstLine,this.shadowRadius-x,yb-y);
        }
        if(out>1){
          line=this.addLR(line,'transparent',(this.shadowRadius-x)+'px');
          x=this.shadowRadius; // done
        }else{
          if(!isIn){
            // fix a strange IE bug where the 12ths recursion seems to get lost...
            if(RUZEE.isIE&&x==this.shadowRadius-12) line=this.addLR(line);
            line=this.addLR(line,rzC2S(co));
          }
          if(setBgImg) this.setBgImg(line,this.shadowRadius-x,yb-y+1);
        }
      }
      e.appendChild(line);
    }
    var ce={ el:e, ps:top?this.psT:this.psB };
    if(top) ce.inSh=this.inSh;
    RUZEE.Borders.cache[this.cacheID+'.'+top]=ce;
    return e;
  },

  /** Create the left and right of the border */
  crLR:function(e){
    var coBgInS=rzC2S(this.coBgIn);
    var coBS=rzC2S(this.coBorder);
    if(this.wBorder>0) e=this.addLR(e,coBS,this.wBorder+'px');
    for(var x=this.shadowPadding; x<this.shadowRadius; ++x){
      coS=rzC2S(rzBlend(this.coShadow,this.coBgOut,x/this.shadowRadius));
      e=this.addLR(e,coS);
    }
    return e;
  },

  setEdges:function(ed){
    ed=ed?ed.toLowerCase():'lrtb';
    this.isL=ed.indexOf('l')>=0;
    this.isR=ed.indexOf('r')>=0;
    this.isT=ed.indexOf('t')>=0;
    this.isB=ed.indexOf('b')>=0;
  },

  /** Calculate the border around e */
  calc:function(e){
    RUZEE.isXHTML=typeof window.RUZEE.isXHTML != 'undefined'
      ?window.RUZEE.isXHTML
      :(/html\:/.test(document.getElementsByTagName('body')[0].nodeName));

    if(!e) return;
    if(e.constructor==Array){
      for(var i=0; i<e.length; ++i) this.calc(e[i]);
      return;
    }
    this.inSh=0;

    // Get the bg image
    this.imgBgInURL=rzGetStyle(e,'background-image',false,null);
    if(this.imgBgInURL&&this.imgBgInURL=='none') this.imgBgInURL=null;
    if(this.imgBgInURL){
      this.imgBgInRepeat=rzGetStyle(e,'background-repeat',false,null);
    }
    this.coBgIn=rzS2C(rzGetStyle(e,'background-color'),'#ffffff');
    this.coBgOut=rzS2C(rzGetStyle(e.parentNode,'background-color'),'#ffffff');
    var borderCSS='border-'+(this.isT?'top-':'bottom-');
    var bs=rzGetStyle(e,borderCSS+'style',false,'none');
    if(bs && bs!='' && bs!='none' && bs!='hidden'){
      this.coBorder=rzS2C(rzGetStyle(e,borderCSS+'color',false,'black'));
      this.wBorder=rzPX2I(rzGetStyle(e,borderCSS+'width',false,'1px'));
    }else{
      this.coBorder=this.coBgIn;
      this.wBorder=0;
    }
    this.coShadow=this.coShadowS=='.fade'?this.coBorder:rzS2C(this.coShadowS);

    this.cacheID=
      rzC2S(this.coBgIn)+'.'+rzC2S(this.coBgOut)+'.'+
      rzC2S(this.coBorder)+'.'+rzC2S(this.coShadow)+'.'+
      this.wBorder+'.'+this.isL+this.isR+this.isT+this.isB+'.'+
      this.cornerRadius+'.'+this.shadowRadius+'.'+
      this.shadowPadding+'.'+this.shadowShift+'.'+
      this.imgBgInURL+'.'+this.imgBgInRepeat;

    var wr=this.crDiv();
    var cwr=this.crDiv();

    this.psT=0;
    this.psB=0;
    if(this.isT) wr.appendChild(this.crTB(true));
    wr.appendChild(this.crLR(cwr));
    if(this.isB) wr.appendChild(this.crTB(false));
    var psLR=this.shadowRadius-this.shadowPadding+this.wBorder;
    var psL=this.isL?psLR:0;
    var psR=this.isR?psLR:0;
    var isTB=this.isT&&this.isB;
    if(!isTB)this.inSh=0;
    var psT=isTB?Math.floor((this.psT+this.psB+this.inSh)/2):this.psT+Math.floor(this.inSh/2);
    var psB=this.psB+this.psT+this.inSh-psT;

    var cwrbg=cwr;
    // lift the inner div up if necessary
    if(this.inSh!=0){
      var up1=this.crDiv(); cwr.appendChild(up1);
      var up2=this.crDiv(); up1.appendChild(up2);
      cwr.style.position=up1.style.position='relative';
      up1.style.top=up2.style.marginBottom=this.inSh+'px';
      if(RUZEE.isIE) cwr.style.height='1%';
      cwrbg=up1; cwr=up2;
    }

    this.setBgImg(cwrbg,psL,this.psT+this.inSh);
    cwrbg.style.backgroundColor=rzC2S(this.coBgIn);

    if(RUZEE.isIE){
      e.style.height=cwr.style.height='1%'; // fix IE 3px jog when floated
    }else{
      // work around for other browsers for sebs problem
      var end=this.crDiv(null,'1px');
      end.style.marginBottom='-1px';
      e.appendChild(end);
      cwr.appendChild(end.cloneNode(true));
    }
    if(this.height>0) cwr.style.height=(RUZEE.isStrict?this.height:(this.height-this.psB-this.psT))+'px';

    var funcs=[
      rzUpdatePad(e,wr,cwr,'top',psT),
      rzUpdatePad(e,wr,cwr,'bottom',psB),
      rzUpdatePad(e,wr,cwr,'left',psL),
      rzUpdatePad(e,wr,cwr,'right',psR)];

    var ewr=null;
    var tn=e.tagName.toLowerCase();
    if(tn=='img'||tn=='table'){
      ewr=this.crDiv(); //ewr.style.display='inline';
    }

    RUZEE.Borders.addCalc(function(){
      for(var i=0; i<funcs.length; ++i) funcs[i]();
      e.style.background='transparent';
      e.style.backgroundImage='none';
      if(ewr){
        e.parentNode.replaceChild(ewr,e);
        ewr.appendChild(wr);
        cwr.appendChild(e);
      }else{
        e.appendChild(wr);
        while (e.childNodes.length>1){
          cwr.appendChild(e.removeChild(e.childNodes[0]));
        }
      }
    });
  },

  /** Render the border around e */
  render:function(e){
    this.calc(e);
    RUZEE.Borders.renderCalcs();
  },

  // DEPRECATED STUFF - WILL BE REMOVED IN ONE OF THE NEXT RELEASES!
  draw:function(e,edges){
    this.setEdges(edges?edges.toLowerCase():'lrtb');
    if(typeof e=='string'){
      if(e.charAt(0)!='.') e='#'+e;
      e=RUZEE.Borders.cssQuery(e);
    }
    this.render(e);
  }
}; // of Border prototype

// add an event handler for render() if RUZEE.Events are available
if(RUZEE.Events){
  RUZEE.Events.add(window,'domload',function(){
    if(RUZEE.Borders.autoRender) RUZEE.Borders.render();
  });
}

// internal tools

Math.sqr=function(x){
  return x*x;
};

function rzCC(s){
  for(var exp=/-([a-z])/; exp.test(s); s=s.replace(exp,RegExp.$1.toUpperCase()));
  return s;
};

function rzGetStyle(e,a,transOk,d){
  if(e==null) return d;
  if(typeof e=='string') e=document.getElementById(e);
  var v=null;
  if(document.defaultView){
    var cs=document.defaultView.getComputedStyle(e,null);
    if (!cs && window.getComputedStyle) cs=window.getComputedStyle(e,null);
    if(cs){
      v=cs.getPropertyValue(a);
      if(!v && cs.getPropertyCSSValue){
        v=cs.getPropertyCSSValue(a);
        if(v) v=v.getStringValue();
      }
    }
  }

  if(!v && e.currentStyle){
    v=e.currentStyle[rzCC(a)];
    if (!v) v=e.currentStyle[a];
  }

  if(!v && e.style) v=e.style[rzCC(a)];
  // KHTML bug fix: transparent is #000000 - if you want black, use #010101 in your CSS.
  // Safari work around: transparent is 'rgba(0, 0, 0, 0)'
  if(!transOk && v && (v.toLowerCase()=='transparent' || v=='#000000' || v=='rgba(0, 0, 0, 0)')) v=null;
  return v?v:d?d:e==document.body?d:rzGetStyle(e.parentNode,a);
};

function rzPX2I(px){
  if(!px) return 0;
  var p=/\s*(\d\d*)px/.exec(px);
  if(p) return parseInt(p[1]);
  return 0;
};

  /** Update the padding of s depending of the setting of d and subtract subPx */
function rzUpdatePad(org,newo,newi,l,subPx,isSet){
  var padL='padding-'+l; var padCC=rzCC(padL);
  var marL='margin-'+l; var marCC=rzCC(marL);
  var borL='border-'+l+'-width'; var borCC=rzCC(borL);

  var pad=rzGetStyle(org,padL);
  var bor=rzGetStyle(org,borL);
  var r=rzPX2I(pad)+rzPX2I(bor);
  var v=r-subPx;
  v=(v<0?0:v)+'px';

  if(RUZEE.isStrict){
    newo.style[marCC]=(-r)+'px';
    newi.style[padCC]=v;
    return function(){
      org.style[borCC]='0px';
      org.style[padCC]=r+'px';
    };
  }else{
    newi.style[padCC]=v;
    return function(){
      org.style[borCC]=org.style[padCC]='0px';
    };
  }
};

function rzS2C(s,d){
    if (!s) return d?rzS2C(d):[0,0,0,0];
    if (s.charAt(0)=='#') s=s.substr(1,6);
    s=s.replace(/ /g,'').toLowerCase();
    // The CSS 2.1 colors
    var COLORS = {
         aqua:'00ffff', black:'000000', blue:'0000ff', fuchsia:'ff00ff',
         gray:'808080', green:'008000', lime:'00ff00', maroon:'800000',
         navy:'000080', olive:'808000', orange:'ffa500', purple:'800080',
         red:'ff0000', silver:'c0c0c0', teal:'008080', white:'ffffff',
         yellow:'ffff00'
    };
    for (var key in COLORS) if (s==key) s=COLORS[key];

    var p=/^rgba\((\d{1,3}),\s*(\d{1,3}),\s*(\d{1,3}),\s*(\d{1,3})\)$/.exec(s);
    if(p) return [parseInt(p[1]),parseInt(p[2]),parseInt(p[3]),parseInt(p[4])];
    var p=/^rgb\((\d{1,3}),\s*(\d{1,3}),\s*(\d{1,3})\)$/.exec(s);
    if(p) return [parseInt(p[1]),parseInt(p[2]),parseInt(p[3]),255];
    p=/^(\w{2})(\w{2})(\w{2})$/.exec(s);
    if(p) return [parseInt(p[1],16),parseInt(p[2],16),parseInt(p[3],16),255];
    p=/^(\w{1})(\w{1})(\w{1})$/.exec(s);
    if(p) return [parseInt(p[1]+p[1],16),parseInt(p[2]+p[2],16),parseInt(p[3]+p[3],16),255];
    return d?rzS2C(d):[0,0,0,0];
};

function rzC2S(c){
  if(typeof c=='string') return c;
  r='0'+c[0].toString(16);
  g='0'+c[1].toString(16);
  b='0'+c[2].toString(16);
  return '#'
    +r.substring(r.length-2)
    +g.substring(g.length-2)
    +b.substring(b.length-2);
};

function rzBlend(a,b,w){
  return Array(
    Math.round(a[0]+(b[0]-a[0])*w),
    Math.round(a[1]+(b[1]-a[1])*w),
    Math.round(a[2]+(b[2]-a[2])*w),
    Math.round(a[3]+(b[3]-a[3])*w));
};

// DEPRECATED STUFF - WILL BE REMOVED IN ONE OF THE NEXT RELEASES!
function rzCrSimpleBorder(rad){
  return new RUZEE.Borders.Border({ borderType:'simple', cornerRadius:rad });
};

function rzCrShadowBorder(rad,smar,coShadowS){
  return new RUZEE.Borders.Border({
    borderType:'shadow', cornerRadius:rad, shadowWidth:smar, shadowColor:coShadowS });
};

function rzCrFadeBorder(rad){
  return new RUZEE.Borders.Border({ borderType:'fade', cornerRadius:rad });
};

function rzCrGlowBorder(rad,gmar,coGlowS){
  return new RUZEE.Borders.Border({ borderType:'glow', cornerRadius:rad, glowWidth:gmar, glowColor:coGlowS });
};

function rzGetElementsByClass(c,n,t) {
  return RUZEE.getElementsByClass(c,t);
};
