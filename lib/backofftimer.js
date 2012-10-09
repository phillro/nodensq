/**
 * User: philliprosen
 * Date: 10/9/12
 * Time: 4:46 PM
 */


function BackOffTimer(min_interval, max_interval, ratio, short_length, long_length) {
  this.min_interval = min_interval;
  this.max_interval = max_interval;
  this.ratio = this.ratio || .25;
  this.short_length = short_length || 10;
  this.long_length = long_length || 250;

  this.min_interval = _Decimal(min_interval)
  this.max_interval = _Decimal(max_interval)

  this.max_short_timer = (this.max_interval - this.min_interval) * ratio;
  this.max_long_timer = (this.max_interval - this.min_interval) * (1 - ratio)
  this.short_unit = this.max_short_timer / short_length;
  this.long_unit = this.max_long_timer / long_length;
  this.short_interval = 0;
  this.long_interval = 0;
}

BackOffTimer.prototype.success = function () {
  //Update the timer to reflect a successfull call
  this.short_interval -= this.short_unit
  this.long_interval -= this.long_unit
  this.short_interval = max(this.short_interval, Decimal(0))
  this.long_interval = max(this.long_interval, Decimal(0))
}

BackOffTimer.prototype.failure = function () {
  //Update the timer to reflect a failed call
  this.short_interval += this.short_unit
  this.long_interval += this.long_unit
  this.short_interval = this.short_interval<this.max_short_timer ?this.short_interval : this.max_short_timer;
  this.long_interval = this.long_interval<this.max_long_timer ? this.long_interval : this.max_long_timer;
}

BackOffTimer.prototype.get_interval = function () {
  return this.min_interval + this.short_interval + this.long_interval;
}

exports.BackOffTimer = BackOffTimer;